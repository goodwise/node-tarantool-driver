function AutoPipeline() {}

AutoPipeline.prototype._processAutoPipeliningQueue = function () {
    var concatenatedBuffer = Buffer.concat(this.autoPipelineQueue);
    this.autoPipelineQueue = [];
    this.socket.write(concatenatedBuffer);
    this.autoPipeliningStarted = false;
};

AutoPipeline.prototype._addToAutoPipeliningQueue = function (buffer) {
    this.autoPipelineQueue.push(buffer);
    // check if auto pipelining is already started in order to don't execute 'setImmediate' each time
    if (!this.autoPipeliningStarted) {
        setImmediate(this._processAutoPipeliningQueue.bind(this))
        this.autoPipeliningStarted = true;
    }
};

module.exports = AutoPipeline;