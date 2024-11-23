function AutoPipeline() {}

AutoPipeline.prototype._processAutoPipeliningQueue = function (self) {
    var concatenatedBuffer = Buffer.concat(self.autoPipelineQueue);
    self.autoPipelineQueue = [];
    self.autoPipeliningId = 0;
    self.socket.write(concatenatedBuffer);
};

AutoPipeline.prototype._addToAutoPipeliningQueue = function (buffer) {
    this.autoPipelineQueue.push(buffer);
    if (!this.autoPipeliningId) {
        this.autoPipeliningId = setTimeout(this._processAutoPipeliningQueue, this.options.autoPipeliningPeriod, this);
    }
};

module.exports = AutoPipeline;