var debug = require('debug')('tarantool-driver:handler');
var {TarantoolError} = require('./utils');
var {UnpackrStream} = require('msgpackr');

exports.connectHandler = function (self) {
	return function () {
		self.retryAttempts = 0;
		switch(self.state){
			case self.states.CONNECTING:
				self.dataState = self.states.PREHELLO;
				break;
			case self.states.CONNECTED:
				if(self.options.password){
					self.setState(self.states.AUTH);
					self._auth(self.options.username, self.options.password)
						.then(function(){
							self.setState(self.states.CONNECT, {host: self.options.host, port: self.options.port});
							debug('authenticated [%s]', self.options.username);
							sendOfflineQueue.call(self);
						}, function(err){
							self.flushQueue(err);
							self.errorHandler(err);
							self.disconnect(true);
						});
				} else {
					self.setState(self.states.CONNECT, {host: self.options.host, port: self.options.port});
					sendOfflineQueue.call(self);
				}
				break;
		}
  };
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

function createUnpackrStream () {
	var self = this;
	var decodedHeaders = {};
	var decodingStep = 0;

	var unpackrStream = new UnpackrStream({
		mapsAsObjects: true,
		int64AsType: "auto",
	});

	unpackrStream.on("error", function (error) {
		self.errorHandler(
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
				self.processResponse(decodedHeaders, data);
				decodingStep = 2;
				break;
			default:
				self.errorHandler(
					new TarantoolError(
						"Unknown step detected while decoding response data, maybe network loss occured with some bytes?"
					)
				);
			}
			break;
		default:
			self.errorHandler(
				new TarantoolError(
					"Type of decoded value does not satisfy requirements: " + type
				)
			);
		}
	});

	return unpackrStream;
}

exports.dataHandler = function (self) {
	var unpackrStream = createUnpackrStream.call(self);
	return function (data) {
		switch (self.dataState) {
		case self.states.PREHELLO:
			self.salt = data.toString("utf8").split('\n')[1];
			self.dataState = self.states.CONNECTED;
			self.setState(self.states.CONNECTED);
			exports.connectHandler(self)();
			break;
		case self.states.CONNECTED:
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

exports.closeHandler = function (self) {
	function close () {
		self.setState(self.states.END);
		self.flushQueue(new TarantoolError('Connection is closed.'));
  	}

	return function(){
		process.nextTick(self.emit.bind(self, 'close'));
		if (self.manuallyClosing) {
			self.manuallyClosing = false;
			debug('skip reconnecting since the connection is manually closed.');
			return close();
		}
		if (typeof self.options.retryStrategy !== 'function') {
			debug('skip reconnecting because `retryStrategy` is not a function');
			return close();
		}
		var retryDelay = self.options.retryStrategy(++self.retryAttempts);

		if (typeof retryDelay !== 'number') {
			debug('skip reconnecting because `retryStrategy` doesn\'t return a number');
			return close();
		}
		self.setState(self.states.RECONNECTING, retryDelay);
		if (self.options.reserveHosts) {
			if (self.retryAttempts-1 == self.options.beforeReserve){
				self.useNextReserve();
				self.connect().catch(function(){});
				return;
			}
		}
		debug('reconnect in %sms', retryDelay);

		self.reconnectTimeout = setTimeout(function () {
			self.reconnectTimeout = null;
			self.connect().catch(function(){});
		}, retryDelay);
	};
};