var {
	createBuffer,
	TarantoolError,
} = require('./utils');
var msgpackr = require('msgpackr');
var packr = new msgpackr.Packr();

function _getScaleByNumber (num) {
	if (num <= 255) {
		return 8;
	} else if (num <= 65535) {
		return 16;
	} else if (num <= 4294967295) {
		return 32;
	} else if (num <= Number.MAX_SAFE_INTEGER) {
		return 64;
	} else {
		throw new TarantoolError('Exeeded max supported number for scale');
	}
};

function _getBufferLenghtByNumber (num) {
	if (num <= 255) return 2;
	if (num <= 65535) return 3;
	if (num <= 4294967295) return 5;
	if (num <= Number.MAX_SAFE_INTEGER) return 9;

	throw new TarantoolError('Exeeded max supported buffer length');
};

function _getFbyteByNumber (num) {
	if (num <= 255) return 0xcc;
	if (num <= 65535) return 0xcd;
	if (num <= 4294967295) return 0xce;
	if (num <= Number.MAX_SAFE_INTEGER) return 0xcf;

	throw new TarantoolError('Exeeded max supported buffer length');
};

function _getWriteMethodByNumber (num) {
	if (num <= 255) return 'writeUInt8';
	if (num <= 65535) return 'writeUInt16BE';
	if (num <= 4294967295) return 'writeUInt32BE';
	if (num <= Number.MAX_SAFE_INTEGER) return 'writeBigUInt64BE';

	throw new TarantoolError('Exeeded max supported buffer length');
}

// reuse buffers and pre-write the first byte
var buffer2bytes = createBuffer(2);
buffer2bytes[0] = 0xcc;
var buffer3bytes = createBuffer(3);
buffer3bytes[0] = 0xcd;
var buffer5bytes = createBuffer(5);
buffer5bytes[0] = 0xce;
var buffer9bytes = createBuffer(9);
buffer9bytes[0] = 0xcf;
function _encodeMsgpackNumber (num, scale = 0) {
	if (scale === 0) scale = _getScaleByNumber(num);

	switch (scale) {
		case 8:
			buffer2bytes.writeUInt8(num, 1);
			return buffer2bytes;
		case 16:
			buffer3bytes.writeUInt16BE(num, 1);
			return buffer3bytes;
		case 32:
			buffer5bytes.writeUInt32BE(num, 1);
			return buffer5bytes;
		case 64:
			buffer9bytes.writeBigUInt64BE(num, 1);
			return buffer9bytes;
		default: 
			throw new TarantoolError('Unsupported scale provided: ' + scale);
	};
};

var bufferCache = new Map();

module.exports.getCachedMsgpackBuffer = function (value) {
	var search = bufferCache.get(value);
	if (search) {
		return search;
	} else {
		var encoded = packr.encode(value);
		bufferCache.set(value, encoded);
		return encoded;
	};
};