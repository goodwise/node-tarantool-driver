var { states } = require("./const");

// it is faster to reuse the existing buffer than allocating a new one
var preallocBuf = Buffer.allocUnsafe(Buffer.poolSize);
var shouldSetImmediate = true;
var bufferSizeMultiplier = 2;

exports.createBuffer = function createBuffer (size){
  // prevent the old existing buffers from being overwritten
  if (this.enableAutoPipelining || this.nonWritableHostPolicy || !this.socket || !this.socket.writable || (this.state !== states.CONNECT)) {
    return Buffer.allocUnsafe(size);
  }

  // flush preallocated buffer at the end of loop to prevent OOM
  if (shouldSetImmediate) {
    shouldSetImmediate = false;
    setImmediate(function () {
      preallocBuf = Buffer.allocUnsafe(Buffer.poolSize);
      shouldSetImmediate = true;
    });
  }

  // create a bigger buffer
  if (size > preallocBuf.length) {
    preallocBuf = Buffer.allocUnsafe(size * bufferSizeMultiplier);
  }

  return preallocBuf.subarray(0, size);
};

// draft; Performance is a bit worse in autopipelining mode
var offset = 0;
exports.createBuffer2 = function createBuffer (size){
  // flush preallocated buffer at the end of event loop to prevent OOM
  if (shouldSetImmediate) {
    shouldSetImmediate = false;
    setImmediate(function () {
      preallocateBuf(Buffer.poolSize)
      shouldSetImmediate = true;
    });
  }

  // check if should create a bigger buffer
  var fullLen = size + offset;
  if (fullLen > preallocBuf.length) {
    preallocateBuf(fullLen)
  }

  return preallocBuf.subarray(offset, offset += size);
};

function preallocateBuf (size) {
  preallocBuf = Buffer.allocUnsafe(size * bufferSizeMultiplier);
  offset = 0;
}

exports.parseURL = function(str, reserve){
  var result = {};
  if (str.startsWith('/')) {
    result.path = str
    return result
  }
  var parsed = str.split(':');
  if(reserve){
    result.username = null;
    result.password = null;
  }
  switch (parsed.length){
    case 1:
      result.host = parsed[0];
      break;
    case 2:
      result.host = parsed[0];
      result.port = parsed[1];
      break;
    default:
      result.username = parsed[0];
      result.password = parsed[1].split('@')[0];
      result.host = parsed[1].split('@')[1];
      result.port = parsed[2];
  }
  return result;
};

exports.TarantoolError = function (msg, opts) {
  var err = new Error(msg, opts);
  err.name = 'TarantoolError';
  return err;
}

function withResolversPoly () {
  var resolve = Promise.resolve;
  var reject = Promise.reject;
  var promise = new Promise(function (_resolve, _reject) {
    resolve = _resolve;
    reject = _reject;
  });

  return {
    promise,
    resolve, 
    reject
  };
};

function withResolvers () {
  return Promise.withResolvers()
}
exports.withResolvers = Promise.withResolvers ? withResolvers : withResolversPoly