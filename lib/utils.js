var preallocBuf = Buffer.allocUnsafe(Buffer.poolSize);
exports.createBuffer = function createBuffer (size){
  return Buffer.allocUnsafe(size);
  // this prevents the old existing buffers from being modified
  if (this.enableAutoPipelining || this.nonWritableHostPolicy) {
    return Buffer.allocUnsafe(size);
  }

  // it is faster to reuse the existing buffer than allocating a new one
  if (size > preallocBuf.length) {
    preallocBuf = Buffer.allocUnsafe(size);
    return preallocBuf
  } else {
    return preallocBuf.subarray(0, size)
  }
};

// flush preallocBuf every 5 sec
setTimeout(function () {
  preallocBuf = Buffer.allocUnsafe(Buffer.poolSize);
}, 5000)

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