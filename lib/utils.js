// cache the method of Buffer allocation
var createBufferMethod
if (Buffer.allocUnsafe) {
  createBufferMethod = Buffer.allocUnsafe;
} else if (Buffer.alloc) {
  createBufferMethod = Buffer.alloc;
} else {
  createBufferMethod = new Buffer;
}
var preallocatedBuf = createBufferMethod(10000);
exports.createBuffer = function (size){
  // if running in autopipeling mode, this prevents the older buffers from being modified
  if (this.autoPipeliningId) {
    return createBufferMethod(size);
  }

  // it is faster to reuse the existing buffer than allocating a new one
  if (size > preallocatedBuf.length) {
    preallocatedBuf = createBufferMethod(size);
    return preallocatedBuf
  } else {
    return preallocatedBuf.subarray(0, size)
  }
};

// flush preallocatedBuf every 3 sec
setTimeout(function () {
  preallocatedBuf = createBufferMethod(10000);
}, 3000)

// cache the method of Buffer creation using an array of bytes
var bufferFromMethod
if (Buffer.from) {
  bufferFromMethod = Buffer.from;
} else {
  bufferFromMethod = new Buffer;
}
exports.bufferFrom = function (data, encoding) {
	return bufferFromMethod(data, encoding);
}

// 'buf.slice' is deprecated since Node v17.5.0 / v16.15.0, so we should proceed with 'buf.subarray'
exports.bufferSubarrayPoly = Buffer.prototype.subarray ? Buffer.prototype.subarray : Buffer.prototype.slice

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

function withResolversOrig () {
  return Promise.withResolvers()
}
exports.withResolvers = Promise.withResolvers ? withResolversOrig : withResolversPoly