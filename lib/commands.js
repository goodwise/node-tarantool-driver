/* global Promise */
var { createHash } = require('crypto');
var tarantoolConstants = require('./const');
var {
	bufferFrom,
	createBuffer,
	TarantoolError,
	bufferSubarrayPoly
} = require('./utils');
var { encode: msgpackEncode, Decoder: msgpackDecoder, decode: msgpackDecode } = require('msgpack-lite');
var { codec } = require('./msgpack-extensions');
var { getCachedMsgpackBuffer } = require('./buffer-processor');
var msgpackr = require('msgpackr');
var packr = new msgpackr.Packr();

function Commands() {}
// Commands.prototype.sendCommand = function () {};

var maxSmi = 1<<30

Commands.prototype._getRequestId = function(){
	var _id = this._id
	if (_id[0] > maxSmi)
		_id[0] = 1;
	return _id[0]++;
};

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
	}
};

Commands.prototype._getSpaceId = function(name){
	var _this = this;
	return this.select(tarantoolConstants.Space.space, tarantoolConstants.IndexSpace.name, 1, 0,
		'eq', [name], {
			tuplesToObjects: false,
			autoPipeline: false
		})
		.then(function(value){
			if (value && value.length && value[0])
			{
				var spaceId = value[0][0];
				var tupleKeys = value[0][6];
				tupleKeys.map(function (value, index) {
					tupleKeys[index] = value.name
				})

				_this.namespace[name] = _this.namespace[spaceId] = {
					id: spaceId,
					name: name,
					indexes: {},
					tupleKeys
				};
				return spaceId;
			}
			else
			{
				throw new TarantoolError('Cannot read a space name or space is not defined');
			}
		});
};
Commands.prototype._getIndexId = function(spaceId, indexName){
	var _this = this;
	return this.select(tarantoolConstants.Space.index, tarantoolConstants.IndexSpace.indexName, 1, 0,
		'eq', [spaceId, indexName], {
			tuplesToObjects: false,
			autoPipeline: false
		})
		.then(function(value) {
			if (value && value[0] && value[0].length>1) {
				var indexId = value[0][1];
				var space = _this.namespace[spaceId];
				if (space) {
					_this.namespace[space.name].indexes[indexName] = indexId;
					_this.namespace[space.id].indexes[indexName] = indexId;
				}
				return indexId;
			}
			else
				throw new TarantoolError('Cannot read a space name indexes or index is not defined');
		});
};

Commands.prototype.select = function select (spaceId, indexId, limit, offset, iterator, key, opts = {}) {
	var _this = this;
	return new Promise(function(resolve, reject){
		_this.selectCb(spaceId, indexId, limit, offset, iterator, key, resolve, reject, opts);
	});
}

Commands.prototype._getMetadata = function(spaceName, indexName){
	var _this = this;
	var spName = this.namespace[spaceName] // reduce overhead of lookup
	if (spName)
	{
		spaceName = spName.id;
	}
	if (typeof(spName) != 'undefined' && typeof(spName.indexes[indexName])!='undefined')
	{
		indexName = spName.indexes[indexName];
	}
	if (typeof(spaceName)=='string' && typeof(indexName)=='string')
	{
		return this._getSpaceId(spaceName)
			.then(function(spaceId){
				return Promise.all([spaceId, _this._getIndexId(spaceId, indexName)]);
			});
	}
	var promises = [];
	if (typeof(spaceName) == 'string')
		promises.push(this._getSpaceId(spaceName));
	else
		promises.push(spaceName);
	if (typeof(indexName) == 'string')
		promises.push(this._getIndexId(spaceName, indexName));
	else
		promises.push(indexName);
	return Promise.all(promises);
};

Commands.prototype.ping = function(){
	var _this = this;
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		var len = 9;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqPing;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqPing, 
			reqId,
			buffer,
			[resolve, reject]
		);
	});
};

Commands.prototype.begin = function(transactionTimeout, isolationLevel, opts = {}) {
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		if (!_this.streamId) {
			reject(
				new TarantoolError('Cannot find streamId, maybe called method outside of transaction?')
			);
		}
		var reqId = _this._getRequestId();

		var headersMap = new Map();
		headersMap.set(tarantoolConstants.KeysCode.code, tarantoolConstants.RequestCode.rqBegin)
		headersMap.set(tarantoolConstants.KeysCode.sync, reqId)
		headersMap.set(tarantoolConstants.KeysCode.iproto_stream_id, _this.streamId)
		var headersBuffer = packr.encode(headersMap)

		var bodyMap = new Map();
		if (transactionTimeout) {
			bodyMap.set(tarantoolConstants.KeysCode.iproto_timeout, transactionTimeout)
		}
		if (isolationLevel) {
			bodyMap.set(tarantoolConstants.KeysCode.iproto_txn_isolation, isolationLevel)
		}
		var bodyBuffer = packr.encode(bodyMap)

		var dataLengthBuffer = _encodeMsgpackNumber(headersBuffer.length + bodyBuffer.length);
		var concatenatedBuffers = Buffer.concat([dataLengthBuffer, headersBuffer, bodyBuffer])

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqBegin, 
			reqId,
			concatenatedBuffers,
			[resolve, reject],
			_arguments,
			opts._pipelined === true
		);
	});
};

Commands.prototype.commit = function() {
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		if (!_this.streamId) {
			reject(
				new TarantoolError('Cannot find streamId, maybe called method outside of transaction?')
			);
		}
		var reqId = _this._getRequestId();

		var headersMap = new Map();
		headersMap.set(tarantoolConstants.KeysCode.code, tarantoolConstants.RequestCode.rqCommit)
		headersMap.set(tarantoolConstants.KeysCode.sync, reqId)
		headersMap.set(tarantoolConstants.KeysCode.iproto_stream_id, _this.streamId)
		var headersBuffer = packr.encode(headersMap)

		var dataLengthBuffer = _encodeMsgpackNumber(headersBuffer.length);
		var concatenatedBuffers = Buffer.concat([dataLengthBuffer, headersBuffer])

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqCommit, 
			reqId,
			concatenatedBuffers,
			[resolve, reject],
			_arguments
		);
	});
};

Commands.prototype.rollback = function() {
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		if (!_this.streamId) {
			reject(
				new TarantoolError('Cannot find streamId, maybe called method outside of transaction?')
			);
		}
		var reqId = _this._getRequestId();

		var headersMap = new Map();
		headersMap.set(tarantoolConstants.KeysCode.code, tarantoolConstants.RequestCode.rqRollback)
		headersMap.set(tarantoolConstants.KeysCode.sync, reqId)
		headersMap.set(tarantoolConstants.KeysCode.iproto_stream_id, _this.streamId)
		var headersBuffer = packr.encode(headersMap)

		var dataLengthBuffer = _encodeMsgpackNumber(headersBuffer.length);
		var concatenatedBuffers = Buffer.concat([dataLengthBuffer, headersBuffer])

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqRollback, 
			reqId,
			concatenatedBuffers,
			[resolve, reject],
			_arguments
		);
	});
};

Commands.prototype.selectCb = function(spaceId, indexId, limit, offset, iterator, key, success, error, opts = {}){
	if (!(key instanceof Array))
		key = [key];

	var _this = this;

    if (typeof(spaceId) == 'string' && _this.namespace[spaceId])
        spaceId = _this.namespace[spaceId].id;
    if (typeof(indexId)=='string' && _this.namespace[spaceId] && _this.namespace[spaceId].indexes[indexId])
        indexId = _this.namespace[spaceId].indexes[indexId];
    if (typeof(spaceId)=='string' || typeof(indexId)=='string')
    {
        return _this._getMetadata(spaceId, indexId)
            .then(function(info){
                return _this.selectCb(info[0], info[1], limit, offset, iterator, key, success, error, opts);
            })
            .catch(error);
    }

	var reqId = this._getRequestId();
	if (iterator == 'all')
		key = [];
	var bufKey = msgpackr.pack(key);

    var bufferLength = 42+bufKey.length;
	var buffer = createBuffer(bufferLength);

	buffer[0] = 0xce;
	buffer.writeUInt32BE(37+bufKey.length, 1);
	buffer[5] = 0x83;
	buffer[6] = tarantoolConstants.KeysCode.code;
	buffer[7] = tarantoolConstants.RequestCode.rqSelect;
	buffer[8] = tarantoolConstants.KeysCode.sync;
	buffer[9] = 0xce;
	buffer.writeUInt32BE(reqId, 10);
	buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
	buffer[15] = 0xce;
    buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x86;
	buffer[21] = tarantoolConstants.KeysCode.space_id;
	buffer[22] = 0xcd;
	buffer.writeUInt16BE(spaceId, 23);
	buffer[25] = tarantoolConstants.KeysCode.index_id;
	buffer.writeUInt8(indexId, 26);
	buffer[27] = tarantoolConstants.KeysCode.limit;
	buffer[28] = 0xce;
	buffer.writeUInt32BE(limit, 29);
	buffer[33] = tarantoolConstants.KeysCode.offset;
	buffer[34] = 0xce;
	buffer.writeUInt32BE(offset || 0, 35);
	buffer[39] = tarantoolConstants.KeysCode.iterator;
	buffer.writeUInt8(tarantoolConstants.IteratorsType[iterator], 40);
	buffer[41] = tarantoolConstants.KeysCode.key;
	bufKey.copy(buffer, 42);

	this.sendCommand(
		tarantoolConstants.RequestCode.rqSelect, 
		reqId,
		buffer,
		[success, error],
		arguments,
		opts
	);
};

Commands.prototype.delete = function(spaceId, indexId, key){
	var _this = this;
	var _arguments = arguments;
	if (Number.isInteger(key))
		key = [key];
	return new Promise(function (resolve, reject) {
		if (Array.isArray(key))
		{
			if (typeof(spaceId)=='string' || typeof(indexId)=='string')
			{
				return _this._getMetadata(spaceId, indexId)
					.then(function(info){
						return _this.delete(info[0], info[1],  key);
					})
					.then(resolve)
					.catch(reject);
			}
			var reqId = _this._getRequestId();
			var bufKey = msgpackEncode(key, {codec});

            var len = 23+bufKey.length;
			var buffer = createBuffer(5+len);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x83;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = tarantoolConstants.RequestCode.rqDelete;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
			buffer[15] = 0xce;
			buffer.writeUInt32BE(_this.streamId || 0, 16);
			buffer[20] = 0x83;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 21);
			buffer[22] = 0xcd;
			buffer.writeUInt16BE(spaceId, 23);
			buffer[25] = tarantoolConstants.KeysCode.index_id;
			buffer.writeUInt8(indexId, 26);
			buffer[27] = tarantoolConstants.KeysCode.key;
			bufKey.copy(buffer, 28);

			_this.sendCommand(
				tarantoolConstants.RequestCode.rqDelete, 
				reqId,
				buffer,
				[resolve, reject],
				_arguments,
				_this._pipelined === true
			);
		}
		else
			reject(new TarantoolError('need array'));
	});
};

Commands.prototype.update = function(spaceId, indexId, key, ops){
	var _this = this;
	var _arguments = arguments;
	if (Number.isInteger(key))
		key = [key];
	return new Promise(function (resolve, reject) {
		if (Array.isArray(ops) && Array.isArray(key)){
			if (typeof(spaceId)=='string' || typeof(indexId)=='string') {
				return _this._getMetadata(spaceId, indexId)
					.then(function(info){
						return _this.update(info[0], info[1],  key, ops);
					})
					.then(resolve)
					.catch(reject);
			}
			var reqId = _this._getRequestId();
			var bufKey = msgpackEncode(key, {codec});
			var bufOps = msgpackEncode(ops, {codec});

			var len = 24+bufKey.length+bufOps.length;
			var buffer = createBuffer(len+5);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x83;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = tarantoolConstants.RequestCode.rqUpdate;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
			buffer[15] = 0xce;
			buffer.writeUInt32BE(_this.streamId || 0, 16);
			buffer[20] = 0x84;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 21);
			buffer[22] = 0xcd;
			buffer.writeUInt16BE(spaceId, 23);
			buffer[25] = tarantoolConstants.KeysCode.index_id;
			buffer.writeUInt8(indexId, 26);
			buffer[27] = tarantoolConstants.KeysCode.key;
			bufKey.copy(buffer, 28);
			buffer[28+bufKey.length] = tarantoolConstants.KeysCode.tuple;
			bufOps.copy(buffer, 29+bufKey.length);

			_this.sendCommand(
				tarantoolConstants.RequestCode.rqUpdate,
				reqId,
				buffer,
				[resolve, reject],
				_arguments,
				_this._pipelined === true
			);
		}
		else
			reject(new TarantoolError('need array'));
	});
};

Commands.prototype.upsert = function(spaceId, ops, tuple){
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		if (Array.isArray(ops)){
			if (typeof(spaceId)=='string')
			{
				return _this._getMetadata(spaceId, 0)
					.then(function(info){
						return _this.upsert(info[0], ops, tuple);
					})
					.then(resolve)
					.catch(reject);
			}
			var reqId = _this._getRequestId();
			var bufTuple = msgpackEncode(tuple, {codec});
			var bufOps = msgpackEncode(ops, {codec});

			var len = 22+bufTuple.length+bufOps.length;
			var buffer = createBuffer(len+5);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x83;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = tarantoolConstants.RequestCode.rqUpsert;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
			buffer[15] = 0xce;
			buffer.writeUInt32BE(_this.streamId || 0, 16);
			buffer[20] = 0x83;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 21);
			buffer[22] = 0xcd;
			buffer.writeUInt16BE(spaceId, 23);
			buffer[25] = tarantoolConstants.KeysCode.tuple;
			bufTuple.copy(buffer, 26);
			buffer[26+bufTuple.length] = tarantoolConstants.KeysCode.def_tuple;
			bufOps.copy(buffer, 27+bufTuple.length);

			_this.sendCommand(
				tarantoolConstants.RequestCode.rqUpsert, 
				reqId,
				buffer,
				[resolve, reject],
				_arguments,
				_this._pipelined === true
			);
		}
		else
			reject(new TarantoolError('need ops array'));
	});
};

Commands.prototype.eval = function(expression){
	var _this = this;
	var _arguments = arguments;
	var tuple = Array.prototype.slice.call(arguments, 1);
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		var bufExp = msgpackEncode(expression);
		var bufTuple = msgpackEncode(tuple ? tuple : [], {codec});
		var len = 18+bufExp.length + bufTuple.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x83;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqEval;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
		buffer[15] = 0xce;
		buffer.writeUInt32BE(_this.streamId || 0, 16);
		buffer[20] = 0x82;
		buffer.writeUInt8(tarantoolConstants.KeysCode.expression, 21);
		bufExp.copy(buffer, 22);
		buffer[22+bufExp.length] = tarantoolConstants.KeysCode.tuple;
		bufTuple.copy(buffer, 23+bufExp.length);

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqEval, 
			reqId,
			buffer,
			[resolve, reject],
			_arguments,
			_this._pipelined === true
		);
	});
};

Commands.prototype.call = function(functionName){
	var _this = this;
	var _arguments = arguments;
	var tuple = arguments.length > 1 ? Array.prototype.slice.call(arguments, 1) : [];
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		var bufName = msgpackEncode(functionName);
		var bufTuple = msgpackEncode(tuple ? tuple : [], {codec});
		var len = 18+bufName.length + bufTuple.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x83;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqCall;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
		buffer[15] = 0xce;
		buffer.writeUInt32BE(_this.streamId || 0, 16);
		buffer[20] = 0x82;
		buffer.writeUInt8(tarantoolConstants.KeysCode.function_name, 21);
		bufName.copy(buffer, 22);
		buffer[22+bufName.length] = tarantoolConstants.KeysCode.tuple;
		bufTuple.copy(buffer, 23+bufName.length);

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqCall, 
			reqId,
			buffer,
			[resolve, reject],
			_arguments,
			_this._pipelined === true
		);
	});
};

Commands.prototype.sql = function(query, bindParams = []){
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		// var bufParams = msgpackEncode(bindParams, {codec});
		var bufParams = msgpackr.pack(bindParams);
		var isPreparedStatement = (typeof query === 'number') // in case of the statement ID being passed to 'query' param
		var bufQuery = isPreparedStatement ? getCachedMsgpackBuffer(query) : msgpackr.pack(query); // cache only prepared queries, considering them frequently used

		var len = 18+bufQuery.length + bufParams.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x83;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqExecute;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
		buffer[15] = 0xce;
		buffer.writeUInt32BE(_this.streamId || 0, 16);
		buffer[20] = 0x82;
		buffer.writeUInt8(isPreparedStatement ? tarantoolConstants.KeysCode.stmt_id : tarantoolConstants.KeysCode.sql_text, 21);
		bufQuery.copy(buffer, 22);
		buffer[22+bufQuery.length] = tarantoolConstants.KeysCode.sql_bind;
		bufParams.copy(buffer, 23+bufQuery.length);

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqExecute, 
			reqId,
			buffer,
			[resolve, reject],
			_arguments,
			_this._pipelined === true
		);
	});
};

Commands.prototype.prepare = function(query, opts = {}){
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		var bufQuery = msgpackEncode(query);

		var len = 13+bufQuery.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqPrepare;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = 0x82;
		buffer.writeUInt8(tarantoolConstants.KeysCode.sql_text, 15);
		bufQuery.copy(buffer, 16);

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqPrepare, 
			reqId,
			buffer,
			[resolve, reject],
			_arguments,
			opts
		);
	});
};

Commands.prototype.id = function(version = 3, features = [1], auth_type = 'chap-sha1'){
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();

		var headersMap = new Map();
		headersMap.set(tarantoolConstants.KeysCode.code, tarantoolConstants.RequestCode.rqId)
		headersMap.set(tarantoolConstants.KeysCode.sync, reqId)
		var headersBuffer = packr.encode(headersMap)

		var bodyMap = new Map();
		bodyMap.set(tarantoolConstants.KeysCode.iproto_version, version)
		bodyMap.set(tarantoolConstants.KeysCode.iproto_features, features)
		bodyMap.set(tarantoolConstants.KeysCode.iproto_auth_type, auth_type)
		var bodyBuffer = packr.encode(bodyMap)

		var dataLengthBuffer = _encodeMsgpackNumber(headersBuffer.length + bodyBuffer.length);
		var concatenatedBuffers = Buffer.concat([dataLengthBuffer, headersBuffer, bodyBuffer])

		_this.sendCommand(
			tarantoolConstants.RequestCode.rqId, 
			reqId,
			concatenatedBuffers,
			[resolve, reject],
			_arguments
		);
	});
};

Commands.prototype.insert = function(spaceId, tuple){
	var reqId = this._getRequestId();
	return this._replaceInsert(tarantoolConstants.RequestCode.rqInsert, reqId, spaceId, tuple);
};

Commands.prototype.replace = function(spaceId, tuple){
	var reqId = this._getRequestId();
	return this._replaceInsert(tarantoolConstants.RequestCode.rqReplace, reqId, spaceId, tuple);
};

Commands.prototype._replaceInsert = function(cmd, reqId, spaceId, tuple, opts = {}){
	var _this = this;
	var _arguments = arguments;
	return new Promise(function (resolve, reject) {
		if (Array.isArray(tuple)){
			if (typeof(spaceId)=='string')
			{
				return _this._getMetadata(spaceId, 0)
					.then(function(info){
						return _this._replaceInsert(cmd, reqId, info[0], tuple);
					})
					.then(resolve)
					.catch(reject);
			}
			var bufTuple = msgpackEncode(tuple);
			var len = 21+bufTuple.length;
			var buffer = createBuffer(len+5);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x83;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = cmd;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
			buffer[15] = 0xce;
			buffer.writeUInt32BE(_this.streamId || 0, 16);
			buffer[20] = 0x82;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 21);
			buffer[22] = 0xcd;
			buffer.writeUInt16BE(spaceId, 23);
			buffer[25] = tarantoolConstants.KeysCode.tuple;
			bufTuple.copy(buffer, 26);

			_this.sendCommand(
				cmd, 
				reqId,
				buffer,
				[resolve, reject],
				_arguments,
				_this._pipelined === true
			);
		}
		else
			reject(new TarantoolError('need array'));
	});
};

Commands.prototype._auth = function(username, password){
	var _this = this;
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();

		var user = msgpackEncode(username);
		var scrambled = scramble(password, _this.salt);
		var len = 44+user.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqAuth;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = 0x82;
		buffer.writeUInt8(tarantoolConstants.KeysCode.username, 15);
		user.copy(buffer, 16);
		buffer[16+user.length] = tarantoolConstants.KeysCode.tuple;
		buffer[17+user.length] = 0x92;
		tarantoolConstants.passEnter.copy(buffer, 18+user.length);
		buffer[28+user.length] = 0xb4;
		scrambled.copy(buffer, 29+user.length);

		_this.sentCommands.set(reqId, [
			tarantoolConstants.RequestCode.rqAuth,
			[resolve, reject]
		])
		_this.socket.write(buffer);
	});
};

function shatransform(t){
	return createHash('sha1').update(t).digest();
}

function xor(a, b) {
	if (!Buffer.isBuffer(a)) a = bufferFrom(a);
	if (!Buffer.isBuffer(b)) b = bufferFrom(b);
	var res = [];
	var i;
	if (a.length > b.length) {
		for (i = 0; i < b.length; i++) {
			res.push(a[i] ^ b[i]);
		}
	} else {
		for (i = 0; i < a.length; i++) {
			res.push(a[i] ^ b[i]);
		}
	}
	return bufferFrom(res);
}

function scramble(password, salt){
	var encSalt = bufferFrom(salt, 'base64');
	var step1 = shatransform(password);
	var step2 = shatransform(step1);
	var step3 = shatransform(Buffer.concat([encSalt[bufferSubarrayPoly](0, 20), step2]));
	return xor(step1, step3);
}

module.exports = Commands;