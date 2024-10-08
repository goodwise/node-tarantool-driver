/* global Promise */
var { createHash } = require('crypto');
var tarantoolConstants = require('./const');
var {
	bufferFrom,
	createBuffer,
	TarantoolError,
	bufferSubarrayPoly
} = require('./utils');
var { encode: msgpackEncode } = require('msgpack-lite');
var { codec } = require('./msgpack-extensions');

function Commands() {}
Commands.prototype.sendCommand = function () {};

var maxSmi = 1<<30

Commands.prototype._getRequestId = function(){
  if (this._id > maxSmi)
    this._id =0;
  return this._id++;
};

Commands.prototype._getSpaceId = function(name){
	var _this = this;
	return this.select(tarantoolConstants.Space.space, tarantoolConstants.IndexSpace.name, 1, 0,
		'eq', [name])
		.then(function(value){
			if (value && value.length && value[0])
			{
				var spaceId = value[0][0];
				_this.namespace[name] = {
					id: spaceId,
					name: name,
					indexes: {}
				};
				_this.namespace[spaceId] = {
					id: spaceId,
					name: name,
					indexes: {}
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
		'eq', [spaceId, indexName])
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
Commands.prototype.select = function(spaceId, indexId, limit, offset, iterator, key, _isPipelined) {
	var _this = this;
	if (!(key instanceof Array))
		key = [key];
	return new Promise(function(resolve, reject){
		if (typeof(spaceId) == 'string' && _this.namespace[spaceId])
			spaceId = _this.namespace[spaceId].id;
		if (typeof(indexId)=='string' && _this.namespace[spaceId] && _this.namespace[spaceId].indexes[indexId])
			indexId = _this.namespace[spaceId].indexes[indexId];
		if (typeof(spaceId)=='string' || typeof(indexId)=='string')
		{
			return _this._getMetadata(spaceId, indexId)
				.then(function(info){
					return _this.select(info[0], info[1], limit, offset, iterator, key, _isPipelined);
				})
				.then(resolve)
				.catch(reject);
		}
		var reqId = _this._getRequestId();

		if (iterator == 'all')
			key = [];
		var bufKey = msgpackEncode(key, {codec});
		var len = 31+bufKey.length;
		var buffer = createBuffer(5+len);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqSelect;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = 0x86;
		buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 15);
		buffer[16] = 0xcd;
		buffer.writeUInt16BE(spaceId, 17);
		buffer[19] = tarantoolConstants.KeysCode.index_id;
		buffer.writeUInt8(indexId, 20);
		buffer[21] = tarantoolConstants.KeysCode.limit;
		buffer[22] = 0xce;
		buffer.writeUInt32BE(limit, 23);
		buffer[27] = tarantoolConstants.KeysCode.offset;
		buffer[28] = 0xce;
		buffer.writeUInt32BE(offset, 29);
		buffer[33] = tarantoolConstants.KeysCode.iterator;
		buffer.writeUInt8(tarantoolConstants.IteratorsType[iterator], 34);
		buffer[35] = tarantoolConstants.KeysCode.key;
		bufKey.copy(buffer, 36);

		_this.sendCommand([
			tarantoolConstants.RequestCode.rqSelect, 
			reqId, 
			{resolve: resolve, reject: reject}
		], 
		buffer,
		_isPipelined === true
	);
	});
};

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

		_this.sendCommand([
			tarantoolConstants.RequestCode.rqPing, 
			reqId, 
			{resolve: resolve, reject: reject}
		], buffer);
	});
};

Commands.prototype.selectCb = function(spaceId, indexId, limit, offset, iterator, key, success, error){
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
                return _this.selectCb(info[0], info[1], limit, offset, iterator, key, success, error);
            })
            .catch(error);
    }

	var reqId = this._getRequestId();
	if (iterator == 'all')
		key = [];
	var bufKey = msgpackEncode(key, {codec});
	var len = 31+bufKey.length;
	var buffer = createBuffer(5+len);

	buffer[0] = 0xce;
	buffer.writeUInt32BE(len, 1);
	buffer[5] = 0x82;
	buffer[6] = tarantoolConstants.KeysCode.code;
	buffer[7] = tarantoolConstants.RequestCode.rqSelect;
	buffer[8] = tarantoolConstants.KeysCode.sync;
	buffer[9] = 0xce;
	buffer.writeUInt32BE(reqId, 10);
	buffer[14] = 0x86;
	buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 15);
	buffer[16] = 0xcd;
	buffer.writeUInt16BE(spaceId, 17);
	buffer[19] = tarantoolConstants.KeysCode.index_id;
	buffer.writeUInt8(indexId, 20);
	buffer[21] = tarantoolConstants.KeysCode.limit;
	buffer[22] = 0xce;
	buffer.writeUInt32BE(limit, 23);
	buffer[27] = tarantoolConstants.KeysCode.offset;
	buffer[28] = 0xce;
	buffer.writeUInt32BE(offset, 29);
	buffer[33] = tarantoolConstants.KeysCode.iterator;
	buffer.writeUInt8(tarantoolConstants.IteratorsType[iterator], 34);
	buffer[35] = tarantoolConstants.KeysCode.key;
	bufKey.copy(buffer, 36);

	this.sendCommand([
		tarantoolConstants.RequestCode.rqSelect, 
		reqId, 
		{resolve: success, reject: error}
	], buffer);
};

Commands.prototype.delete = function(spaceId, indexId, key){
	var _this = this;
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

			var len = 17+bufKey.length;
			var buffer = createBuffer(5+len);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x82;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = tarantoolConstants.RequestCode.rqDelete;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = 0x83;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 15);
			buffer[16] = 0xcd;
			buffer.writeUInt16BE(spaceId, 17);
			buffer[19] = tarantoolConstants.KeysCode.index_id;
			buffer.writeUInt8(indexId, 20);
			buffer[21] = tarantoolConstants.KeysCode.key;
			bufKey.copy(buffer, 22);

			_this.sendCommand([
				tarantoolConstants.RequestCode.rqDelete, 
				reqId, 
				{resolve: resolve, reject: reject}
			], buffer);
		}
		else
			reject(new TarantoolError('need array'));
	});
};

Commands.prototype.update = function(spaceId, indexId, key, ops){
	var _this = this;
	if (Number.isInteger(key))
		key = [key];
	return new Promise(function (resolve, reject) {
		if (Array.isArray(ops) && Array.isArray(key)){
			if (typeof(spaceId)=='string' || typeof(indexId)=='string')
			{
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

			var len = 18+bufKey.length+bufOps.length;
			var buffer = createBuffer(len+5);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x82;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = tarantoolConstants.RequestCode.rqUpdate;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = 0x84;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 15);
			buffer[16] = 0xcd;
			buffer.writeUInt16BE(spaceId, 17);
			buffer[19] = tarantoolConstants.KeysCode.index_id;
			buffer.writeUInt8(indexId, 20);
			buffer[21] = tarantoolConstants.KeysCode.key;
			bufKey.copy(buffer, 22);
			buffer[22+bufKey.length] = tarantoolConstants.KeysCode.tuple;
			bufOps.copy(buffer, 23+bufKey.length);

			_this.sendCommand([
				tarantoolConstants.RequestCode.rqUpdate, 
				reqId, 
				{resolve: resolve, reject: reject}
			], buffer);
		}
		else
			reject(new TarantoolError('need array'));
	});
};

Commands.prototype.upsert = function(spaceId, ops, tuple){
	var _this = this;
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

			var len = 16+bufTuple.length+bufOps.length;
			var buffer = createBuffer(len+5);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x82;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = tarantoolConstants.RequestCode.rqUpsert;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = 0x83;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 15);
			buffer[16] = 0xcd;
			buffer.writeUInt16BE(spaceId, 17);
			buffer[19] = tarantoolConstants.KeysCode.tuple;
			bufTuple.copy(buffer, 20);
			buffer[20+bufTuple.length] = tarantoolConstants.KeysCode.def_tuple;
			bufOps.copy(buffer, 21+bufTuple.length);

			_this.sendCommand([
				tarantoolConstants.RequestCode.rqUpsert, 
				reqId, 
				{resolve: resolve, reject: reject}
			], buffer);
		}
		else
			reject(new TarantoolError('need ops array'));
	});
};


Commands.prototype.eval = function(expression){
	var _this = this;
	var tuple = Array.prototype.slice.call(arguments, 1);
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		var bufExp = msgpackEncode(expression);
		var bufTuple = msgpackEncode(tuple ? tuple : [], {codec});
		var len = 12+bufExp.length + bufTuple.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqEval;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = 0x82;
		buffer.writeUInt8(tarantoolConstants.KeysCode.expression, 15);
		bufExp.copy(buffer, 16);
		buffer[16+bufExp.length] = tarantoolConstants.KeysCode.tuple;
		bufTuple.copy(buffer, 17+bufExp.length);

		_this.sendCommand([
			tarantoolConstants.RequestCode.rqEval, 
			reqId, 
			{resolve: resolve, reject: reject}
		], buffer);
	});
};

Commands.prototype.call = function(functionName){
	var _this = this;
	var tuple = arguments.length > 1 ? Array.prototype.slice.call(arguments, 1) : [];
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		var bufName = msgpackEncode(functionName);
		var bufTuple = msgpackEncode(tuple ? tuple : [], {codec});
		var len = 12+bufName.length + bufTuple.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqCall;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = 0x82;
		buffer.writeUInt8(tarantoolConstants.KeysCode.function_name, 15);
		bufName.copy(buffer, 16);
		buffer[16+bufName.length] = tarantoolConstants.KeysCode.tuple;
		bufTuple.copy(buffer, 17+bufName.length);

		_this.sendCommand([
			tarantoolConstants.RequestCode.rqCall, 
			reqId, 
			{resolve: resolve, reject: reject}
		], buffer);
	});
};

Commands.prototype.sql = function(query, bindParams = []){
	var _this = this;
	return new Promise(function (resolve, reject) {
		var reqId = _this._getRequestId();
		var bufQuery = msgpackEncode(query);

		var bufParams = msgpackEncode(bindParams, {codec});
		var len = 12+bufQuery.length + bufParams.length;
		var buffer = createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqExecute;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);
		buffer[14] = 0x82;
		buffer.writeUInt8(tarantoolConstants.KeysCode.sql_text, 15);
		bufQuery.copy(buffer, 16);
		buffer[16+bufQuery.length] = tarantoolConstants.KeysCode.sql_bind;
		bufParams.copy(buffer, 17+bufQuery.length);

		_this.sendCommand([
			tarantoolConstants.RequestCode.rqExecute, 
			reqId, 
			{resolve: resolve, reject: reject}
		], buffer);
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

Commands.prototype._replaceInsert = function(cmd, reqId, spaceId, tuple){
	var _this = this;
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
			var len = 15+bufTuple.length;
			var buffer = createBuffer(len+5);

			buffer[0] = 0xce;
			buffer.writeUInt32BE(len, 1);
			buffer[5] = 0x82;
			buffer[6] = tarantoolConstants.KeysCode.code;
			buffer[7] = cmd;
			buffer[8] = tarantoolConstants.KeysCode.sync;
			buffer[9] = 0xce;
			buffer.writeUInt32BE(reqId, 10);
			buffer[14] = 0x82;
			buffer.writeUInt8(tarantoolConstants.KeysCode.space_id, 15);
			buffer[16] = 0xcd;
			buffer.writeUInt16BE(spaceId, 17);
			buffer[19] = tarantoolConstants.KeysCode.tuple;
			bufTuple.copy(buffer, 20);

			_this.sendCommand([
				cmd, 
				reqId, 
				{resolve: resolve, reject: reject}
			], buffer);
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

		_this.commandsQueue.push([
			tarantoolConstants.RequestCode.rqAuth, 
			reqId, 
			{resolve: resolve, reject: reject}, 
		]);
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