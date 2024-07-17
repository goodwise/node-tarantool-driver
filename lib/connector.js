/* global Promise */
var net = require('net');
var tls = require('tls');
var { TarantoolError } = require('./utils');

function Connector(options) {
  this.options = options;
}

Connector.prototype.disconnect = function () {
  this.connecting = false;
  if (this.socket) {
    this.socket.end();
  }
};

Connector.prototype.connect = function (callback) {
  this.connecting = true;

  var _this = this;
  process.nextTick(function () {
    if (!_this.connecting) {
      callback(new TarantoolError('Connection is closed.'));
      return;
    }
    try {
      var connectionModule
      if (typeof _this.options.tls == 'object') {
        connectionModule = tls
        _this.options = Object.assign(_this.options, _this.options.tls)
      } else {
        connectionModule = net
      }
      _this.socket = connectionModule.connect(_this.options);
    } catch (err) {
      callback(err);
      return;
    }
    callback(null, _this.socket);
  });
};

module.exports = Connector;