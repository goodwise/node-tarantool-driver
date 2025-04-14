var commandsPrototype = require('./commands');
var PipelineResponse = require('./pipeline-response');

function commandInterceptorFactory (method) {
    return function commandInterceptor () {
        this.pipelinedCommands.push([method, arguments]);
        return this
    }
}

var commandsPrototypeKeys = Object.keys(commandsPrototype);
var setOfCommandInterceptors = {};
for (var commandKey of commandsPrototypeKeys) {
    setOfCommandInterceptors[commandKey] = commandInterceptorFactory(commandKey)
}

function sendCommandInterceptor (requestCode, reqId, buffer, callbacks, commandArguments, opts) {
    // If the Select request was made to search for a system space/index name, then bypass this
    if (opts.autoPipeline === false) {
        return this._parent.sendCommand(requestCode, reqId, buffer, callbacks, commandArguments, opts)
    }

    this._buffersConcatenedCounter++
    this.buffers.push(buffer)

    opts._pipelined = true;
    // firstly add the command to 'sentCommands' Map
    var send = this._parent.sendCommand(requestCode, reqId, buffer, callbacks, commandArguments, opts)
    // then write the buffer
    // in order to avoid the possible race conditions (?)
    this._trySendConcatenedBuffer()

    return send;
}

function exec () {
    var _this = this;
    var promises = [];
    for (var interceptedCommand of this.pipelinedCommands) {
        var args = interceptedCommand[1]
        var method = interceptedCommand[0]
        promises.push(
            new Promise(function (resolve) {
                _this._originalMethods[method].apply(_this._originalMethods, args)
                .then(function (result) {
                    resolve([null, result])
                })
                .catch(function (error) {
                    _this._buffersConcatenedCounter++ // fake, because rejected promise doesn't have its request buffer
                    _this._trySendConcatenedBuffer()
                    resolve([error, null])
                })
            })
        );
    }

    return new Promise(function (resolve) {
        Promise.all(promises)
        .then(function (result) {
            resolve(
                new PipelineResponse(result)
            )
        })
    })
}

class Pipeline {
    constructor (self) {
        var _this = this;
        this._parent = self;
        this.buffers = [];
        this.pipelinedCommands = [];
        this._buffersConcatenedCounter = 0;
        this._originalMethods = Object.assign({}, 
            commandsPrototype,
            self,
            {
                _id: self._id,
                sendCommand: sendCommandInterceptor.bind(_this)
            }
        )
        this.exec = exec;
        Object.assign(this, setOfCommandInterceptors);
    };

    flushPipelined () {
        this.pipelinedCommands = [];
        this.buffers = [];
    };

    _trySendConcatenedBuffer () {
        if (this._buffersConcatenedCounter == this.pipelinedCommands.length) {
            this._parent.socket.write(Buffer.concat(this.buffers))
            this.flushPipelined()
        }
    };
}

module.exports.pipeline = function () {
    return new Pipeline(this);
};