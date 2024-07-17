var commandsPrototype = require('./commands').prototype;
var {RequestCode} = require('./const');
var {TarantoolError} = require('./utils');

var commandsPrototypeKeys = Object.keys(commandsPrototype)

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

        self.buffersConcatenedCounter++
        if (self.pipelinedBuffer) {
            self.pipelinedBuffer = Buffer.concat([self.pipelinedBuffer, buffer])
        } else {
            self.pipelinedBuffer = buffer
        }

        if (self.buffersConcatenedCounter >= self.pipelinedCommands.length) {
            self._parent.socket.write(self.pipelinedBuffer)
            self.flushPipelined()
        }

        self._parent.sendCommand(command, buffer, true)
    }
}

function exec () {
    var _this = this;
    return new Promise(function (resolve, reject) {
        if (_this.pipelinedCommands.length == 0) return reject(new TarantoolError('Nothing to execute, probably none of methods were evoked'))
        var promises = []
        for (var interceptedCommand of _this.pipelinedCommands) {
            var args = interceptedCommand[1]
            if (interceptedCommand[0] == 'select') {
                args = Object.values(args) // Otherwise 'arguments' doesn't get applied to function below correctly
                args[6] = true // Marks 'select' request as pipelined, otherwise '_getMetadata' will break the pipelined queue
            }
            promises.push(
                _this._originalMethods[interceptedCommand[0]].apply(_this._originalMethods, args)
            );
        }

        Promise.all(promises)
        .then(resolve)
        .catch(reject);
    });
}

function flushPipelined () {
    this.pipelinedCommands = [];
}

function Pipeline (self) {
    var _this = this;
    this._parent = self;
    this.pipelinedCommands = [];
    this.buffersConcatenedCounter = 0;
    this._originalMethods = Object.assign({}, 
        commandsPrototype,
        self,
        {
            _id: self._id,
            sendCommand: sendCommandInterceptorFactory(_this)
        }
    )
    this.exec = exec
    this.flushPipelined = flushPipelined
    Object.assign(this, setOfCommandInterceptors);
}

Pipeline.prototype.pipeline = function () {
    return new Pipeline(this);
}

module.exports = Pipeline;