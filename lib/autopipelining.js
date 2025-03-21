function AutoPipeline() {}

AutoPipeline.prototype._processAutoPipeliningQueue = function () {
    var concatenatedBuffer = Buffer.concat(this.autoPipelineQueue);
    this.autoPipelineQueue = [];
    this.autoPipeliningId = 0;
    this.socket.write(concatenatedBuffer);
    // check if this feature is still enabled and continue processing if so
    if (this.autoPipeliningEnabled) setImmediate(this._processAutoPipeliningQueue.bind(this));
};

AutoPipeline.prototype._addToAutoPipeliningQueue = function (buffer) {
    this.autoPipelineQueue.push(buffer);
    // check if auto pipelining is already started in order to don't run 'setImmediate' each time
    if (!this.autoPipeliningStarted) {
        setImmediate(this._processAutoPipeliningQueue.bind(this))
        this.autoPipeliningStarted = true;
    }
};

module.exports = AutoPipeline;