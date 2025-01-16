var {
    prototype: commandsPrototype
} = require('./commands');

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

function sendCommandInterceptorFactory (self) {
    return function sendCommandInterceptor (requestCode, reqId, buffer, callbacks, commandArguments, opts) {
        // If the Select request was made to search for a system space/index name, then bypass this
        if (opts.autoPipeline === false) {
            return self._parent.sendCommand(requestCode, reqId, buffer, callbacks, commandArguments, opts)
        }

        self._buffersConcatenedCounter++
        if (self.pipelinedBuffer) {
            self.pipelinedBuffer = Buffer.concat([self.pipelinedBuffer, buffer])
        } else {
            self.pipelinedBuffer = buffer
        }

        opts._pipelined = true;
        // firstly add the command to 'sentCommands' Map
        var send = self._parent.sendCommand(requestCode, reqId, buffer, callbacks, commandArguments, opts)
        // then write the buffer
        // in order to avoid the possible race conditions (?)
        self._trySendConcatenedBuffer()

        return send;
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
            result.findPipelineError = findPipelineError
            result.findPipelineErrors = findPipelineErrors
            resolve(result)
        })
    })
}

function findPipelineError () {
    var error = this.find(element => element[0])
    if (error !== undefined) {
      return error[0]
    } else {
      return null;
    }
};
  
function findPipelineErrors () {
    var aoe = [];
    for (var subarray of this) {
        var errored_element = subarray[0]
        if (errored_element) aoe.push(errored_element)
    }

    return aoe;
};

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

module.exports.pipeline = function () {
    return new Pipeline(this);
};