var debug = require('debug')('tarantool-driver:handler');
var {TarantoolError} = require('./utils');
var {UnpackrStream} = require('msgpackr');

exports.connectHandler = function () {
		this.retryAttempts = 0;
		switch(this.state){
			case this.states.CONNECTING:
				this.dataState = this.states.PREHELLO;
				break;
			case this.states.CONNECTED:
				if(this.options.password){
					this.setState(this.states.AUTH);
					this._auth(this.options.username, this.options.password)
						.then(function(){
							this.setState(this.states.CONNECT, {host: this.options.host, port: this.options.port});
							debug('authenticated [%s]', this.options.username);
							sendOfflineQueue.call(this);
						}, function(err){
							this.flushQueue(err);
							this.errorHandler(err);
							this.disconnect(true);
						});
				} else {
					this.setState(this.states.CONNECT, {host: this.options.host, port: this.options.port});
					sendOfflineQueue.call(this);
				}
				break;
		}
};

function sendOfflineQueue(){
	if (this.offlineQueue.length) {
		debug('send %d commands in offline queue', this.offlineQueue.length);
		var offlineQueue = this.offlineQueue;
		this.resetOfflineQueue();
		while (offlineQueue.length > 0) {
			var command = offlineQueue.shift();
			this.sendCommand.apply(this, command);
		}
	}
}

const unpackrOpts = {
	mapsAsObjects: true,
	int64AsType: "auto",
};

function createUnpackrStream () {
	var _this = this;
	var decodedHeaders = {};
	var decodingStep = 0;

	var unpackrStream = new UnpackrStream(unpackrOpts);

	unpackrStream.on("error", function (error) {
		_this.errorHandler(
			new TarantoolError("Msgpack unpacker errored", {
				cause: error,
			})
		);
	});

	unpackrStream.on("data", function (data) {
		var type = typeof data;
		switch (type) {
		case "number":
			decodingStep = 0;
			break;
		case "object":
			switch (decodingStep) {
			case 0:
				decodedHeaders = data;
				decodingStep = 1;
				break;
			case 1:
				_this.processResponse(decodedHeaders, data);
				decodingStep = 2;
				break;
			default:
				_this.errorHandler(
					new TarantoolError(
						"Unknown step detected while decoding response data, maybe network loss occured with some bytes?"
					)
				);
			}
			break;
		default:
			_this.errorHandler(
				new TarantoolError(
					"Type of decoded value does not satisfy requirements: " + type
				)
			);
		}
	});

	return unpackrStream;
}

exports.dataHandler = function () {
	var unpackrStream = createUnpackrStream.call(this);
	return (data) => {
		switch (this.dataState) {
		case this.states.PREHELLO:
			this.salt = data.toString("utf8").split('\n')[1];
			this.dataState = this.states.CONNECTED;
			this.setState(this.states.CONNECTED);
			exports.connectHandler.call(this);
			break;
		case this.states.CONNECTED:
			unpackrStream.write(data)
			break;
		}
	};
};

function errorHandler (error){
	debug('error: %s', error);
	this.silentEmit('error', error);
};
exports.errorHandler = errorHandler;

exports.closeHandler = function () {
	function close () {
		this.setState(this.states.END);
		this.flushQueue(new TarantoolError('Connection is closed.'));
  	}

	return function(){
		process.nextTick(this.emit.bind(this, 'close'));
		if (this.manuallyClosing) {
			this.manuallyClosing = false;
			debug('skip reconnecting since the connection is manually closed.');
			return close();
		}
		if (typeof this.options.retryStrategy !== 'function') {
			debug('skip reconnecting because `retryStrategy` is not a function');
			return close();
		}
		var retryDelay = this.options.retryStrategy(++this.retryAttempts);

		if (typeof retryDelay !== 'number') {
			debug('skip reconnecting because `retryStrategy` doesn\'t return a number');
			return close();
		}
		this.setState(this.states.RECONNECTING, retryDelay);
		if (this.options.reserveHosts) {
			if (this.retryAttempts-1 == this.options.beforeReserve){
				this.useNextReserve();
				this.connect().catch(function(){});
				return;
			}
		}
		debug('reconnect in %sms', retryDelay);

		this.reconnectTimeout = setTimeout(function () {
			this.reconnectTimeout = null;
			this.connect().catch(function(){});
		}, retryDelay);
	};
};