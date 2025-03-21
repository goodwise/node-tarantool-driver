const {EventEmitter} = require('events');
const debug = require('debug')('tarantool-driver:main');
const {
    pick,
    defaults,
    assign,
    noop
} = require('lodash');
const { 
    parseURL, 
    TarantoolError,
    withResolvers,
    createBuffer
} = require('./utils');
const msgpackExt = require('./msgpack-extensions');
const tarantoolConstants = require('./const');
const Commands = require('./commands');
const Pipeline = require('./pipeline');
const Transaction = require('./transaction');
const Parser = require('./parser');
const Connector = require('./connector');
const AutoPipeline = require('./autopipelining');
const eventHandler = require('./event-handler');

const revertStates = {
    0: 'connecting',
    1: 'connected',
    2: 'awaiting',
    4: 'inited',
    8: 'prehello',
    16: 'awaiting_length',
    32: 'end',
    64: 'reconnecting',
    128: 'auth',
    256: 'connect',
    512: 'changing_host'
};

const states = {
    CONNECTING: 0,
    CONNECTED: 1,
    AWAITING: 2,
    INITED: 4,
    PREHELLO: 8,
    AWAITING_LENGTH: 16,
    END: 32,
    RECONNECTING: 64,
    AUTH: 128,
    CONNECT: 256,
    CHANGING_HOST: 512
};

const defaultOptions = {
    host: 'localhost',
    port: 3301,
    path: null,
    username: null,
    password: null,
    reserveHosts: [],
    beforeReserve: 2,
    timeout: 0,
    noDelay: true,
    requestTimeout: 0,
    keepAlive: true,
    tuplesToObjects: false,
    nonWritableHostPolicy: null, /* What to do when Tarantool server rejects write operation, 
    e.g. because of box.cfg.read_only set to 'true' or during fetching snapshot.
    Possible values are: 
    - null: reject Promise
    - 'changeHost': disconnect from the current host and connect to the next of 'reserveHosts'. Pending Promise will be rejected.
    - 'changeAndRetry': same as 'changeHost', but after connecting tries to run the command again in order to fullfil the Promise
    */
    maxRetriesPerRequest: 5, // If 'nonWritableHostPolicy' specified, Promise will be rejected only after exceeding this setting
    enableOfflineQueue: true,
    retryStrategy: function (times) {
        return Math.min(times * 50, 2000);
    },
    lazyConnect: false
};

class TarantoolConnection extends EventEmitter {
    constructor () {
        super();
        this.reserve = [];
        this.connecting = false;
        this.socket = null;
        this.parseOptions = parseOptions;
        this.options = {};
        this.parseOptions(arguments[0], arguments[1], arguments[2]);
        this.schemaId = 0;
        this.states = states;
        this.schemas = {};
        this.dataState = this.states.PREHELLO;
        this.commandsQueue = [];
        this.offlineQueue = [];
        this.namespace = {};
        this.retryAttempts = 0;
        this._id = [ 1 ];
        this.sentCommands = new Map();
        this.autoPipelineQueue = [];
        this.autoPipeliningEnabled = false;
        this.autoPipeliningStarted = false;
        this.enableAutoPipelining = this.options.enableAutoPipelining;
        this.resetOfflineQueue = resetOfflineQueue;
        this.useNextReserve = useNextReserve;
        this.sendCommand = sendCommand;
        this.setState = setState;
        this.connect = connect;
        this.flushQueue = flushQueue;
        this.silentEmit = silentEmit;
        this.destroy = this.disconnect = disconnect;
        this.errorHandler = eventHandler.errorHandler;
        this.createBuffer = createBuffer;
        Object.assign(this, Connector);
        Object.assign(this, Commands);
        Object.assign(this, AutoPipeline.prototype);
        Object.assign(this, Parser);
        Object.assign(this, Transaction.prototype);
        Object.assign(this, Pipeline);
        Object.assign(this, msgpackExt);
    
        if (this.options.lazyConnect) {
            this.setState(this.states.INITED);
        } else {
            this.connect().catch(noop);
        }
    }

    set enableAutoPipelining (value = false) {
        if (typeof value != 'boolean') throw new Error(`Should pass a boolean to 'enableAutoPipelining'`);

        this.autoPipeliningEnabled = value;
        this.autoPipeliningStarted = false;
    }

    get enableAutoPipelining () {
        return this.autoPipeliningEnabled;
    }
}

function resetOfflineQueue () {
  this.offlineQueue = [];
};

function parseOptions (){
    var i;
    for (i = 0; i < arguments.length; ++i) {
        var arg = arguments[i];
        if (arg === null || typeof arg === 'undefined') {
            continue;
        }

        switch (typeof arg) {
            case 'object':
                defaults(this.options, arg);
            break;
            case 'string':
                if(!isNaN(arg) && (parseFloat(arg) | 0) === parseFloat(arg)){
                    this.options.port = arg;
                    continue;
                }
                defaults(this.options, parseURL(arg));
            break;
            case 'number':
                this.options.port = arg;
            break;
            default:
                throw new TarantoolError('Invalid argument ' + arg);
        }
    }
    defaults(this.options, defaultOptions);
    var reserveHostsLength = this.options.reserveHosts && this.options.reserveHosts.length || 0
    if ((this.options.nonWritableHostPolicy != null) && (reserveHostsLength == 0)) {
        throw new TarantoolError('\'nonWritableHostPolicy\' option is specified, but there are no reserve hosts. Specify them in connection options via \'reserveHosts\'')
    }
    if (typeof this.options.port === 'string') {
        this.options.port = parseInt(this.options.port, 10);
    }
    if (this.options.path != null) {
        delete this.options.port
        delete this.options.host
    }
    if (reserveHostsLength > 0) {
        this.reserveIterator = 1;
        this.reserve.push(pick(this.options, ['port', 'host', 'username', 'password', 'path']));
        for(i = 0; i<this.options.reserveHosts.length; i++){
            this.reserve.push(parseURL(this.options.reserveHosts[i], true));
        }
    }
    this.options.beforeReserve = this.options.beforeReserve < 0 ? 0 : this.options.beforeReserve;
};

function useNextReserve (){
    this.retryAttempts = 0;
    if(this.reserveIterator == this.reserve.length) this.reserveIterator = 0;
    delete this.options.port
    delete this.options.host
    delete this.options.path
    delete this.options.port
    delete this.options.username
    delete this.options.password
    var reserveOptions = this.reserve[this.reserveIterator++]
    assign(this.options, reserveOptions);

    if (!reserveOptions) throw new TarantoolError('Attempted to use next reserve host, but iseems to be that there are none of them. Specify via the connection configuration.');

    return reserveOptions;
};

function sendCommand (requestCode, reqId, buffer, callbacks, commandArguments, opts = {}){
    // create promise for async methods
    var promise;
    if (!callbacks) {
        var {resolve, reject, ...etc} = withResolvers();
        promise = etc.promise
        callbacks = [resolve, reject];
    };

    switch (this.state){
        case this.states.INITED:
            this.connect().catch(noop);
        case this.states.CONNECT:
            if(!this.socket || !this.socket.writable){
                debug('queue -> %s(%s)', requestCode, reqId);
		        this.offlineQueue.push([
                    requestCode, 
                    reqId, 
                    buffer, 
                    callbacks, 
                    commandArguments, 
                    opts
                ]);
            } else {
                const tuplesToObjects = opts.tuplesToObjects ?? this.options.tuplesToObjects;

                // create an array which will be stored till the response is received
                var setValue = [
                    requestCode,
                    callbacks,
                    [],
                    [],
                    tuplesToObjects
                ];

                if ((this.options.nonWritableHostPolicy === 'changeAndRetry') || tuplesToObjects) setValue[2] = Object.values(commandArguments);

                var requestTimeout = this.options.requestTimeout || opts.requestTimeout;
                if (requestTimeout) setValue[3] = setTimeout(function () {
                    callbacks[1](
                        new TarantoolError('Request timed out (' + requestTimeout + ' ms)')
                    )
                }, requestTimeout);

                this.sentCommands.set(reqId, setValue);
                const shouldAutoPipeline =
                  opts._pipelined === true
                    ? false
                    : opts.autoPipeline ?? this.autoPipeliningEnabled;
                // check if should be autopipelined
                if (shouldAutoPipeline) {
                    this._addToAutoPipeliningQueue(buffer);
                // in manually pipelined mode data is written via its own function
                } else if (!opts._pipelined) {
                    debug(`sending request №${reqId} to server`)
                    this.socket.write(buffer);
                };
            }
            break;
        case this.states.END:
            callbacks[1](new TarantoolError('Connection is closed.'));
            break;
        default:
            debug('queue -> %s(%s)', requestCode, reqId);
            if (!this.options.enableOfflineQueue) {
                return callbacks[1](new TarantoolError('Connection not established yet!'));
            };
		    this.offlineQueue.push([
                requestCode, 
                reqId, 
                buffer, 
                callbacks, 
                commandArguments, 
                opts
            ]);
    }

    return promise;
};

function setState (state, arg) {
    var address;
    if (this.socket && this.socket.remoteAddress && this.socket.remotePort) {
        address = this.socket.remoteAddress + ':' + this.socket.remotePort;
    } else {
        if (this.options.path != null) {
            address = this.options.path
        } else {
            address = this.options.host + ':' + this.options.port;
        }
    }
    debug('state[%s]: %s -> %s', address, revertStates[this.state] || '[empty]', revertStates[state]);
    this.state = state;
    process.nextTick(this.emit.bind(this, revertStates[state], arg));
};

function connect (){
    return new Promise(function (resolve, reject) {
        if (this.state === this.states.CONNECTING || this.state === this.states.CONNECT || this.state === this.states.CONNECTED || this.state === this.states.AUTH) {
            reject(new TarantoolError('Tarantool is already connecting/connected'));
            return;
        }
        this.setState(this.states.CONNECTING);
        var _this = this;
        this._connect(function(err, socket){
            if(err){
                _this.flushQueue(err);
                _this.silentEmit('error', err);
                reject(err);
                _this.setState(_this.states.END);
                return;
            }
            _this.socket = socket;
            socket.once('connect', eventHandler.connectHandler.bind(_this));
            socket.once('error', eventHandler.errorHandler.bind(_this))
            socket.once('close', eventHandler.closeHandler.bind(_this));
            socket.on('data', eventHandler.dataHandler.call(_this));

            if (_this.options.timeout) {
                socket.setTimeout(_this.options.timeout, function () {
                    socket.setTimeout(0);
                    socket.destroy();

                    var error = new TarantoolError('connect ETIMEDOUT');
                    error.errorno = 'ETIMEDOUT';
                    error.code = 'ETIMEDOUT';
                    error.syscall = 'connect';
                    _this.errorHandler(error);
                });
                socket.once('connect', function () {
                    socket.setTimeout(0);
                });
            }
            var connectionConnectHandler = function () {
                _this.removeListener('close', connectionCloseHandler);
                resolve();
            };
            var connectionCloseHandler = function () {
                _this.removeListener('connect', connectionConnectHandler);
                reject(new Error('Connection is closed.'));
            };
            _this.once('connect', connectionConnectHandler);
            _this.once('close', connectionCloseHandler);
        });
    }.bind(this));
};

function flushQueue (error) {
    while (this.offlineQueue.length > 0) {
        this.offlineQueue.shift()[3][1](error);
    }
    while (this.commandsQueue.length > 0) {
        this.commandsQueue.shift()[3][1](error);
    }
};

function silentEmit (eventName) {
  var error;
  if (eventName === 'error') {
    error = arguments[1];

    if (this.status === 'end') {
      return;
    }

    if (this.manuallyClosing) {
      if (
        error instanceof Error &&
        (
          error.message === 'Connection manually closed' ||
          error.syscall === 'connect' ||
          error.syscall === 'read'
        )
      ) {
        return;
      }
    }
  }
  if (this.listeners(eventName).length > 0) {
    return this.emit.apply(this, arguments);
  }
  if (error && error instanceof Error) {
    console.error('[tarantool-driver] Unhandled error event:', error.stack);
  }
  return false;
};

function disconnect (reconnect){
    if (!reconnect) {
        this.manuallyClosing = true;
    }
    if (this.reconnectTimeout) {
        clearTimeout(this.reconnectTimeout);
        this.reconnectTimeout = null;
    }
    if (this.state === this.states.INITED) {
        eventHandler.closeHandler(this)();
    } else {
        this._disconnect();
    }
};

module.exports = TarantoolConnection;