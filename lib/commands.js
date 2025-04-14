/* global Promise */
var { createHash } = require('crypto');
var tarantoolConstants = require('./const');
var {
	TarantoolError,
	withResolvers
} = require('./utils');
var { Packr } = require('msgpackr');
var packr = new Packr({
	variableMapSize: true,
	useRecords: false,
	encodeUndefinedAsNil: true
});

var bufferCache = new Map();
function getCachedMsgpackBuffer (value) {
	var search = bufferCache.get(value);
	if (search) {
		return search;
	} else {
		var encoded = packr.encode(value);
		bufferCache.set(value, encoded);
		return encoded;
	};
};

var maxSmi = 1<<30
exports._getRequestId = function _getRequestId (){
	var _id = this._id
	if (_id[0] > maxSmi) _id[0] = 1;
	return _id[0]++;
};

// exports._loadSchemas = function _loadSchemas () {
// 	return Promise.all([
// 		this.select(281, 0, 2147483647, 0, 'all', [])
// 		.then(

// 		)
// 	])
// }

exports._getMetadata = function _getMetadata (spaceName, indexName){
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

exports._getIndexId = function _getIndexId (spaceId, indexName){
	var _this = this;
	return this.select(tarantoolConstants.Space.index, tarantoolConstants.IndexSpace.indexName, 1, 0,
		'eq', [spaceId, indexName], {
			tupleToObject: false,
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

exports._getSpaceId = function _getSpaceId (name){
	var _this = this;
	return this.select(tarantoolConstants.Space.space, tarantoolConstants.IndexSpace.name, 1, 0,
		'eq', [name], {
			tupleToObject: false,
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

exports.writePacketHeaders = function writePacketHeaders (buffer, length) {
	const reqId = this._getRequestId();

    buffer[0] = 0xce;
	buffer.writeUInt32BE(length, 1);
	buffer[5] = 0x83;
	buffer[6] = tarantoolConstants.KeysCode.code;
	buffer[8] = tarantoolConstants.KeysCode.sync;
	buffer[9] = 0xce;
	buffer.writeUInt32BE(reqId, 10);
	buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id
	buffer[15] = 0xce;
    buffer.writeUInt32BE(this.streamId || 0, 16);

    return reqId;
}

exports.select = function select (spaceId, indexId, limit, offset = 0, iterator = 'eq', key, opts = {}) {
	if (!Array.isArray(key)) key = [key];

	var _this = this;

	if (typeof(spaceId) == 'string' && _this.namespace[spaceId])
        spaceId = _this.namespace[spaceId].id;
    if (typeof(indexId)=='string' && _this.namespace[spaceId] && _this.namespace[spaceId].indexes[indexId])
        indexId = _this.namespace[spaceId].indexes[indexId];
    if (typeof(spaceId)=='string' || typeof(indexId)=='string')
    {
        return _this._getMetadata(spaceId, indexId)
            .then(function(info){
                return _this.select(info[0], info[1], limit, offset, iterator, key, opts);
            })
    }

	if (iterator == 'all') key = [];

	var bufKey = packr.encode(key);
    const len = 37+bufKey.length;
	var buffer = this.createBuffer(len+5);

    const reqId = this.writePacketHeaders(buffer, len);
	buffer[7] = tarantoolConstants.RequestCode.rqSelect;
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
	buffer.writeUInt32BE(offset, 35);
	buffer[39] = tarantoolConstants.KeysCode.iterator;
	buffer.writeUInt8(tarantoolConstants.IteratorsType[iterator], 40);
	buffer[41] = tarantoolConstants.KeysCode.key;
	bufKey.copy(buffer, 42);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqSelect, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
}

exports.ping = function ping (opts = {}){
	var reqId = this._getRequestId();
	var len = 9;
	var buffer = this.createBuffer(len+5);

	buffer[0] = 0xce;
	buffer.writeUInt32BE(len, 1);
	buffer[5] = 0x82;
	buffer[6] = tarantoolConstants.KeysCode.code;
	buffer[7] = tarantoolConstants.RequestCode.rqPing;
	buffer[8] = tarantoolConstants.KeysCode.sync;
	buffer[9] = 0xce;
	buffer.writeUInt32BE(reqId, 10);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqPing, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.begin = function begin (transTimeoutSec = 60.01 /* prevent JS from converting 60.0 to 60 */, isolationLevel = 0, opts = {}) {
	if (!this.streamId) {
		reject(
			new TarantoolError('Cannot find streamId, maybe called method outside of transaction?')
		);
	}

	var reqId = this._getRequestId();
	// check if not decimal
	if (Number.isInteger(transTimeoutSec)) transTimeoutSec += 0.001;
	var transTimeoutBuf = packr.encode(transTimeoutSec)

	var len = 19+transTimeoutBuf.length;
	var buffer = this.createBuffer(5+len);

	buffer[0] = 0xce;
	buffer.writeUInt32BE(len, 1);
	buffer[5] = 0x83;
	buffer[6] = tarantoolConstants.KeysCode.code;
	buffer[7] = tarantoolConstants.RequestCode.rqBegin;
	buffer[8] = tarantoolConstants.KeysCode.sync;
	buffer[9] = 0xce;
	buffer.writeUInt32BE(reqId, 10);
	buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id;
	buffer[15] = 0xce;
	buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x82;
	buffer[21] = tarantoolConstants.KeysCode.iproto_txn_isolation;
	buffer.writeUInt8(isolationLevel, 22);
	buffer[23] = tarantoolConstants.KeysCode.iproto_timeout;
	transTimeoutBuf.copy(buffer, 24)

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqBegin,
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.commit = function commit (opts = {}) {
	if (!this.streamId) {
		reject(
			new TarantoolError('Cannot find streamId, maybe called method outside of transaction?')
		);
	}
	var reqId = this._getRequestId();

	var len = 14;
	var buffer = this.createBuffer(5+len);

	buffer[0] = 0xce;
	buffer.writeUInt32BE(len, 1);
	buffer[5] = 0x83;
	buffer[6] = tarantoolConstants.KeysCode.code;
	buffer[7] = tarantoolConstants.RequestCode.rqCommit;
	buffer[8] = tarantoolConstants.KeysCode.sync;
	buffer[9] = 0xce;
	buffer.writeUInt32BE(reqId, 10);
	buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id;
	buffer[15] = 0xce;
	buffer.writeUInt32BE(this.streamId || 0, 16);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqCommit, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.rollback = function rollback (opts = {}) {
	if (!this.streamId) {
		reject(
			new TarantoolError('Cannot find streamId, maybe called method outside of transaction?')
		);
	}
	var reqId = this._getRequestId();

	var len = 14;
	var buffer = this.createBuffer(5+len);

	buffer[0] = 0xce;
	buffer.writeUInt32BE(len, 1);
	buffer[5] = 0x83;
	buffer[6] = tarantoolConstants.KeysCode.code;
	buffer[7] = tarantoolConstants.RequestCode.rqRollback;
	buffer[8] = tarantoolConstants.KeysCode.sync;
	buffer[9] = 0xce;
	buffer.writeUInt32BE(reqId, 10);
	buffer[14] = tarantoolConstants.KeysCode.iproto_stream_id;
	buffer[15] = 0xce;
	buffer.writeUInt32BE(this.streamId, 16);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqRollback, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.selectCb = function selectCb (spaceId, indexId, limit, offset, iterator, key, success, error, opts = {}){
	if (!Array.isArray(key)) key = [key];

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
	var bufKey = packr.encode(key);

    var bufferLength = 42+bufKey.length;
	var buffer = this.createBuffer(bufferLength);

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

exports.delete = function _delete (spaceId, indexId, key, opts = {}){
	var _this = this;
	if (Number.isInteger(key)) key = [key];
	if (!Array.isArray(key)) return Promise.reject(new TarantoolError('need array'));
	if (typeof(spaceId)=='string' || typeof(indexId)=='string') {
		return this._getMetadata(spaceId, indexId)
		.then(function(info){
			return _this.delete(info[0], info[1],  key, opts);
		})
	}
	var reqId = this._getRequestId();
	var bufKey = packr.encode(key);

	var len = 23+bufKey.length;
	var buffer = this.createBuffer(5+len);

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
	buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x83;
	buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 21);
	buffer[22] = 0xcd;
	buffer.writeUInt16BE(spaceId, 23);
	buffer[25] = tarantoolConstants.KeysCode.index_id;
	buffer.writeUInt8(indexId, 26);
	buffer[27] = tarantoolConstants.KeysCode.key;
	bufKey.copy(buffer, 28);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqDelete, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.update = function update (spaceId, indexId, key, ops, opts = {}){
	if (Number.isInteger(key)) key = [key];
	if (!(Array.isArray(ops) && Array.isArray(key))) return Promise.reject(new TarantoolError('need array'));

	var _this = this;

	if (typeof(spaceId)=='string' || typeof(indexId)=='string') {
		return this._getMetadata(spaceId, indexId)
			.then(function(info){
				return _this.update(info[0], info[1], key, ops, opts);
			})
	}
	var reqId = this._getRequestId();
	var bufKey = packr.encode(key);
	var bufOps = packr.encode(ops);

	var len = 24+bufKey.length+bufOps.length;
	var buffer = this.createBuffer(len+5);

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
	buffer.writeUInt32BE(this.streamId || 0, 16);
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

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqUpdate,
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.upsert = function upsert (spaceId, ops, tuple, opts = {}){
	var _this = this;
	if (!Array.isArray(ops)) return Promise.reject(new TarantoolError('need ops array'));
	if (typeof(spaceId)=='string') {
		return this._getMetadata(spaceId, 0)
			.then(function(info){
				return _this.upsert(info[0], ops, tuple, opts);
			})
	}
	var reqId = this._getRequestId();
	var bufTuple = packr.encode(tuple);
	var bufOps = packr.encode(ops);

	var len = 22+bufTuple.length+bufOps.length;
	var buffer = this.createBuffer(len+5);

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
	buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x83;
	buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 21);
	buffer[22] = 0xcd;
	buffer.writeUInt16BE(spaceId, 23);
	buffer[25] = tarantoolConstants.KeysCode.tuple;
	bufTuple.copy(buffer, 26);
	buffer[26+bufTuple.length] = tarantoolConstants.KeysCode.def_tuple;
	bufOps.copy(buffer, 27+bufTuple.length);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqUpsert, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.eval = function eval (expression, opts = {}){
	var tuple = Array.prototype.slice.call(arguments, 1);
	var reqId = this._getRequestId();
	var bufExp = getCachedMsgpackBuffer(expression);
	var bufTuple = packr.encode(tuple ? tuple : []);
	var len = 18+bufExp.length + bufTuple.length;
	var buffer = this.createBuffer(len+5);

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
	buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x82;
	buffer.writeUInt8(tarantoolConstants.KeysCode.expression, 21);
	bufExp.copy(buffer, 22);
	buffer[22+bufExp.length] = tarantoolConstants.KeysCode.tuple;
	bufTuple.copy(buffer, 23+bufExp.length);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqEval, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.call = function call (functionName, opts = {}){
	var tuple = arguments.length > 1 ? Array.prototype.slice.call(arguments, 1) : [];
	var reqId = this._getRequestId();
	var bufName = getCachedMsgpackBuffer(functionName);
	var bufTuple = packr.encode(tuple ? tuple : []);
	var len = 18+bufName.length + bufTuple.length;
	var buffer = this.createBuffer(len+5);

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
	buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x82;
	buffer.writeUInt8(tarantoolConstants.KeysCode.function_name, 21);
	bufName.copy(buffer, 22);
	buffer[22+bufName.length] = tarantoolConstants.KeysCode.tuple;
	bufTuple.copy(buffer, 23+bufName.length);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqCall, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.sql = function sql (query, bindParams = [], opts = {}){
	var reqId = this._getRequestId();
	var bufParams = packr.encode(bindParams);
	var isPreparedStatement = (typeof query === 'number') // in case of the statement ID being passed to 'query' param
	var bufQuery = isPreparedStatement ? getCachedMsgpackBuffer(query) : packr.encode(query); // cache only prepared queries, considering them to be frequently used

	var len = 18+bufQuery.length + bufParams.length;
	var buffer = this.createBuffer(len+5);

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
	buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x82;
	buffer.writeUInt8(isPreparedStatement ? tarantoolConstants.KeysCode.stmt_id : tarantoolConstants.KeysCode.sql_text, 21);
	bufQuery.copy(buffer, 22);
	buffer[22+bufQuery.length] = tarantoolConstants.KeysCode.sql_bind;
	bufParams.copy(buffer, 23+bufQuery.length);

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqExecute, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.sql = function sql (query, bindParams = [], opts = {}){
	var _this = this;
	var _arguments = arguments;
	var {promise, resolve, reject} = withResolvers();

	var reqId = _this._getRequestId();
	var bufParams = packr.encode(bindParams);
	var isPreparedStatement = (typeof query === 'number') // in case of the statement ID being passed to 'query' param
	var bufQuery = isPreparedStatement ? getCachedMsgpackBuffer(query) : packr.encode(query); // cache only prepared queries, considering them frequently used

	var len = 18+bufQuery.length + bufParams.length;
	var buffer = this.createBuffer(len+5);

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
		opts
	);

	return promise;
};

exports.prepare = function prepare (query, opts = {}){
	var reqId = this._getRequestId();
	var bufQuery = packr.encode(query);

	var len = 13+bufQuery.length;
	var buffer = this.createBuffer(len+5);

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

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqPrepare, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports.id = function id (version = 3, features = [1], auth_type = 'chap-sha1', opts = {}){
	var reqId = this._getRequestId();

	var headersMap = new Map();
	headersMap.set(tarantoolConstants.KeysCode.code, tarantoolConstants.RequestCode.rqId)
	headersMap.set(tarantoolConstants.KeysCode.sync, reqId)
	var headersBuffer = packr.encode(headersMap)

	var bodyMap = new Map();
	bodyMap.set(tarantoolConstants.KeysCode.iproto_version, version)
	bodyMap.set(tarantoolConstants.KeysCode.iproto_features, features)
	bodyMap.set(tarantoolConstants.KeysCode.iproto_auth_type, auth_type)
	var bodyBuffer = packr.encode(bodyMap)

	var dataLengthBuffer = packr.encode(headersBuffer.length + bodyBuffer.length);
	var concatenatedBuffers = Buffer.concat([dataLengthBuffer, headersBuffer, bodyBuffer])

	return this.sendCommand(
		tarantoolConstants.RequestCode.rqId, 
		reqId,
		concatenatedBuffers,
		null,
		arguments,
		opts
	);
};

exports.insert = function insert (spaceId, tuple, opts = {}){
	return this._replaceInsert(tarantoolConstants.RequestCode.rqInsert, spaceId, tuple, opts);
};

exports.replace = function replace (spaceId, tuple, opts = {}){
	return this._replaceInsert(tarantoolConstants.RequestCode.rqReplace, spaceId, tuple, opts);
};

exports._replaceInsert = function _replaceInsert (cmd, spaceId, tuple, opts = {}){
	if (!Array.isArray(tuple)) return Promise.reject(new TarantoolError('need array'));

	var _this = this;

	if (typeof(spaceId)=='string')
	{
		return this._getMetadata(spaceId, 0)
			.then(function(info){
				return _this._replaceInsert(cmd, info[0], tuple, opts);
			})
	}

	var reqId = this._getRequestId();
	var bufTuple = packr.encode(tuple);
	var len = 21+bufTuple.length;
	var buffer = this.createBuffer(len+5);

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
	buffer.writeUInt32BE(this.streamId || 0, 16);
	buffer[20] = 0x82;
	buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 21);
	buffer[22] = 0xcd;
	buffer.writeUInt16BE(spaceId, 23);
	buffer[25] = tarantoolConstants.KeysCode.tuple;
	bufTuple.copy(buffer, 26);

	return this.sendCommand(
		cmd, 
		reqId,
		buffer,
		null,
		arguments,
		opts
	);
};

exports._auth = function _auth (username, password){
	var _this = this;
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();

		var user = packr.encode(username);
		var scrambled = scramble(password, _this.salt);
		var len = 44+user.length;
		var buffer = _this.createBuffer(len+5);

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
	if (!Buffer.isBuffer(a)) a = Buffer.from(a);
	if (!Buffer.isBuffer(b)) b = Buffer.from(b);
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
	return Buffer.from(res);
}

function scramble(password, salt){
	var encSalt = Buffer.from(salt, 'base64');
	var step1 = shatransform(password);
	var step2 = shatransform(step1);
	var step3 = shatransform(
		Buffer.concat(
			[
				encSalt.subarray(0, 20),
				step2
			]
		)
	);
	return xor(step1, step3);
}