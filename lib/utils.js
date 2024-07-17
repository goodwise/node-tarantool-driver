// cache the method of Buffer allocation
var createBufferMethod
if (Buffer.allocUnsafe) {
  createBufferMethod = Buffer.allocUnsafe;
} else if (Buffer.alloc) {
  createBufferMethod = Buffer.alloc;
} else {
  createBufferMethod = new Buffer;
}
exports.createBuffer = function (size){
  return createBufferMethod(size);
};

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
var bufferSubarrayPolyName
if (Buffer.prototype.subarray) {
  bufferSubarrayPolyName = 'subarray'
} else {
  bufferSubarrayPolyName = 'slice'
}
exports.bufferSubarrayPoly = bufferSubarrayPolyName

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

exports.TarantoolError = function(msg){
  Error.call(this);
  this.message = msg;
	this.name = 'TarantoolError';
  if (Error.captureStackTrace) {
		Error.captureStackTrace(this);
	} else {
		this.stack = new Error().stack;
	}
};
exports.TarantoolError.prototype = Object.create(Error.prototype);
exports.TarantoolError.prototype.constructor = Error;