var {
    prototype: commandsPrototype
} = require('./commands');
var {RequestCode} = require('./const');

var commandsPrototypeKeys = Object.keys(commandsPrototype);

function commandInterceptorFactory (method) {
    return function commandInterceptor () {
        this.pipelinedCommands.push([method, arguments]);
        return this
    }
}

var setOfCommandInterceptors = {};
for (var commandKey of commandsPrototypeKeys) {
    setOfCommandInterceptors[commandKey] = commandInterceptorFactory(commandKey)
}

function sendCommandInterceptorFactory (self) {
    return function sendCommandInterceptor (command, buffer, isPipelined) {
        // If the Select request was made to search for a system space/index name, then bypass this
        if ((RequestCode.rqSelect == command[0]) && !isPipelined) {
            self._parent.sendCommand(command, buffer)
            return;
        }

        self._buffersConcatenedCounter++
        if (self.pipelinedBuffer) {
            self.pipelinedBuffer = Buffer.concat([self.pipelinedBuffer, buffer])
        } else {
            self.pipelinedBuffer = buffer
        }

        self._trySendConcatenedBuffer()
        self._parent.sendCommand(command, buffer, true)
    }
}

function _trySendConcatenedBuffer () {
    if (this._buffersConcatenedCounter == this.pipelinedCommands.length) {
        this._parent.socket.write(this.pipelinedBuffer)
        this.flushPipelined()
    }
}

function exec () {
    var _this = this;
    var promises = [];
    for (var interceptedCommand of _this.pipelinedCommands) {
        var args = interceptedCommand[1]
        if (interceptedCommand[0] == 'select') {
            args = Object.values(args) // Otherwise 'arguments' doesn't get applied to function below correctly
            args[6] = true // Marks 'select' request as pipelined, otherwise '_getMetadata' will break the pipelined queue
        }
        promises.push(
            new Promise(function (resolve) {
                _this._originalMethods[interceptedCommand[0]].apply(_this._originalMethods, args)
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

    return Promise.all(promises)
}

function flushPipelined () {
    this.pipelinedCommands = [];
}

function Pipeline (self) {
    var _this = this;
    this._parent = self;
    this.pipelinedCommands = [];
    this._buffersConcatenedCounter = 0;
    this._originalMethods = Object.assign({}, 
        commandsPrototype,
        self,
        {
            _id: self._id,
            sendCommand: sendCommandInterceptorFactory(_this)
        }
    )
    this.exec = exec
    this._trySendConcatenedBuffer = _trySendConcatenedBuffer
    this.flushPipelined = flushPipelined
    Object.assign(this, setOfCommandInterceptors);
}

Pipeline.prototype.pipeline = function () {
    return new Pipeline(this);
}

module.exports = Pipeline;