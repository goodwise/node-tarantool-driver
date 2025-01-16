var { addExtension } = require('msgpackr');
var { 
  TarantoolError,
  createBuffer,
  bufferFrom
} = require('./utils');
var {
  parse: uuidParse,
  stringify: uuidStringify
} = require('uuid');
var {
  Uint64BE,
  Int64BE
} = require("int64-buffer");

var packAs = {};

// Pack big integers correctly (fix for https://github.com/tarantool/node-tarantool-driver/issues/48)
packAs.Integer = function (value) {
  if (!Number.isInteger(value)) throw new TarantoolError("Passed value doesn't seems to be an integer")

  if (value > 2147483647) return Uint64BE(value)
  if (value < -2147483648) return Int64BE(value)

  return value
}

// UUID extension
packAs.Uuid = function TarantoolUuidExt (value) {
  if (!(this instanceof TarantoolUuidExt)) {
    return new TarantoolUuidExt(value)
  }

  this.value = value
}

addExtension({
	Class: packAs.Uuid,
	type: 0x02,
	pack(instance) {
		return uuidParse(instance.value);
	},
	unpack(buffer) {
		return uuidStringify(buffer)
	}
});

// Decimal extension
function isFloat(n){
  return Number(n) === n && n % 1 !== 0;
}

packAs.Decimal = function TarantoolDecimalExt (value) {
  if (!(this instanceof TarantoolDecimalExt)) {
    return new TarantoolDecimalExt(value)
  }

  if (!(Number.isInteger(value) || isFloat(value))) {
    throw new TarantoolError('Passed value cannot be packed as decimal: expected integer or floating number')
  }

  this.value = value
}

function isOdd (number) {
  return number % 2 !== 0;
};

var decimalBuffer = createBuffer(1); // reuse buffer
addExtension({
	Class: packAs.Decimal,
	type: 0x01,
	pack(instance) {
		var strNum = instance.value.toString()
    var rawNum = strNum.replace('-', '')
    var rawNumSplitted1 = rawNum.split('.')[1]
    decimalBuffer.writeInt8(rawNumSplitted1 && rawNum.split('.')[1].length || 0)
    var bufHexed = decimalBuffer.toString('hex')
      + rawNum.replace('.', '')
      + (strNum.startsWith('-') ? 'b' : 'a')

    if (isOdd(bufHexed.length)) {
      bufHexed = bufHexed.slice(0, 2) + '0' + bufHexed.slice(2)
    }

    return bufferFrom(bufHexed, 'hex')
	},
	unpack(buffer) {
		var scale = buffer.readIntBE(0, 1)
    var hex = buffer.toString('hex')

    var sign = ['b', 'd'].includes(hex.slice(-1)) ? '-' : '+'
    var slicedValue = hex.slice(2).slice(0, -1)

    // readDoubleBE
    if (scale > 0) {
      var nScale = scale * -1
      slicedValue = slicedValue.slice(0, nScale) + '.' + slicedValue.slice(nScale)
    }

    return parseFloat(sign + slicedValue)
	}
});

// Datetime extension
var datetimeBuffer = createBuffer(16); // reuse buffer
addExtension({
	Class: Date,
	type: 0x04,
	pack(instance) {
		var seconds = instance.getTime() / 1000 | 0
    var nanoseconds = instance.getMilliseconds() * 1000

    datetimeBuffer.writeBigUInt64LE(BigInt(seconds))
    datetimeBuffer.writeUInt32LE(nanoseconds, 8)
    datetimeBuffer.writeUInt32LE(0, 12)
    /* 
      Node.Js 'Date' doesn't provide nanoseconds, so just using milliseconds.
      tzoffset is set to UTC, and tzindex is omitted.
    */

    return datetimeBuffer;
	},
	unpack(buffer) {
		var time = new Date(parseInt(buffer.readBigUInt64LE(0)) * 1000)

    if (buffer.length > 8) {
      var milliseconds = (buffer.readUInt32LE(8) / 1000 | 0)
      time.setMilliseconds(milliseconds)
    }

    return time;
	}
});

for (var packerName of Object.keys(packAs)) {
  exports['pack' + packerName] = packAs[packerName]
}