var net = require('net');
var tls = require('tls');
var { TarantoolError } = require('./utils');

exports._disconnect = function () {
  this.connecting = false;
  if (this.socket) {
    this.socket.end();
  }
};

exports._connect = function (callback) {
  this.connecting = true;

  var _this = this;
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
};