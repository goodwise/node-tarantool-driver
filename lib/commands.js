/* global Promise */
var msgpack = require('msgpack-lite');
var crypto = require('crypto');
var tarantoolConstants = require('./const');
var utils = require('./utils');

const requestMethods = ['select', 'delete', 'insert', 'replace', 'update', 'eval', 'call', 'upsert'];

function Commands() {}
Commands.prototype.sendCommand = function () {};

Commands.prototype._getRequestId = function(){
  if (this._id > 3000000)
    this._id =0;
  return this._id++;
};

Commands.prototype._getSpaceId = function(name){
	return this.select(tarantoolConstants.Space.space, tarantoolConstants.IndexSpace.name, 1, 0,
		'eq', [name])
		.then(function(value){
			if (value && value.length && value[0])
			{
				var spaceId = value[0][0];
				this.namespace[name] = {
					id: spaceId,
					name: name,
					indexes: {}
				};
				this.namespace[spaceId] = {
					id: spaceId,
					name: name,
					indexes: {}
				};
				return spaceId;
			}
			else
			{
				throw new utils.TarantoolError('Cannot read a space name or space is not defined');
			}
		}.bind(this));
};
Commands.prototype._getIndexId = function(spaceId, indexName){
	return this.select(tarantoolConstants.Space.index, tarantoolConstants.IndexSpace.indexName, 1, 0,
		'eq', [spaceId, indexName])
		.then(function(value) {
			if (value && value[0] && value[0].length>1) {
				var indexId = value[0][1];
				var space = this.namespace[spaceId];
				if (space) {
					this.namespace[space.name].indexes[indexName] = indexId;
					this.namespace[space.id].indexes[indexName] = indexId;
				}
				return indexId;
			}
			else
				throw new utils.TarantoolError('Cannot read a space name indexes or index is not defined');
		}.bind(this));
};
Commands.prototype.select = function(spaceId, indexId, limit, offset, iterator, key){
	if (!(key instanceof Array))
		key = [key];
	return new Promise(function(resolve, reject){
		if (typeof(spaceId) == 'string' && this.namespace[spaceId])
			spaceId = this.namespace[spaceId].id;
		if (typeof(indexId)=='string' && this.namespace[spaceId] && this.namespace[spaceId].indexes[indexId])
			indexId = this.namespace[spaceId].indexes[indexId];
		if (typeof(spaceId)=='string' || typeof(indexId)=='string')
		{
			return this._getMetadata(spaceId, indexId)
				.then(function(info){
					return this.select(info[0], info[1], limit, offset, iterator, key);
				}.bind(this))
				.then(resolve)
				.catch(reject);
		}
		var reqId = this._getRequestId();

		if (iterator == 'all')
			key = [];
		var bufKey = this.msgpack.encode(key);
		var len = 31+bufKey.length;
		var buffer = utils.createBuffer(5+len);

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

		this.sendCommand([tarantoolConstants.RequestCode.rqSelect, reqId, {resolve: resolve, reject: reject}], buffer);
	}.bind(this));
};

Commands.prototype._getMetadata = function(spaceName, indexName){
	if (this.namespace[spaceName])
	{
		spaceName = this.namespace[spaceName].id;
	}
	if (typeof(this.namespace[spaceName]) != 'undefined' && typeof(this.namespace[spaceName].indexes[indexName])!='undefined')
	{
		indexName = this.namespace[spaceName].indexes[indexName];
	}
	if (typeof(spaceName)=='string' && typeof(indexName)=='string')
	{
		return this._getSpaceId(spaceName)
			.then(function(spaceId){
				return Promise.all([spaceId, this._getIndexId(spaceId, indexName)]);
			}.bind(this));
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
	return new Promise(function (resolve, reject) {
		var reqId = this._getRequestId();
		var len = 9;
		var buffer = utils.createBuffer(len+5);

		buffer[0] = 0xce;
		buffer.writeUInt32BE(len, 1);
		buffer[5] = 0x82;
		buffer[6] = tarantoolConstants.KeysCode.code;
		buffer[7] = tarantoolConstants.RequestCode.rqPing;
		buffer[8] = tarantoolConstants.KeysCode.sync;
		buffer[9] = 0xce;
		buffer.writeUInt32BE(reqId, 10);

		this.sendCommand([tarantoolConstants.RequestCode.rqPing, reqId, {resolve: resolve, reject: reject}], buffer);
	}.bind(this));
};

Commands.prototype.selectCb = function(spaceId, indexId, limit, offset, iterator, key, success, error){
	if (!(key instanceof Array))
		key = [key];
	var reqId = this._getRequestId();
	if (iterator == 'all')
		key = [];
	var bufKey = this.msgpack.encode(key);
	var len = 31+bufKey.length;
	var buffer = utils.createBuffer(5+len);

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

	this.sendCommand([tarantoolConstants.RequestCode.rqSelect, reqId, {resolve: success, reject: error}], buffer);
};

Commands.prototype.delete = function(spaceId, indexId, key){
	if (Number.isInteger(key))
		key = [key];
	return new Promise(function (resolve, reject) {
		if (Array.isArray(key))
		{
			if (typeof(spaceId)=='string' || typeof(indexId)=='string')
			{
				return this._getMetadata(spaceId, indexId)
					.then(function(info){
						return this.delete(info[0], info[1],  key);
					}.bind(this))
					.then(resolve)
					.catch(reject);
			}
			var reqId = this._getRequestId();
			var bufKey = this.msgpack.encode(key);

			var len = 17+bufKey.length;
			var buffer = utils.createBuffer(5+len);

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
		
			this.sendCommand([tarantoolConstants.RequestCode.rqDelete, reqId, {resolve: resolve, reject: reject}], buffer);
		}
		else
			reject(new utils.TarantoolError('need array'));
	}.bind(this));
};

Commands.prototype.update = function(spaceId, indexId, key, ops){
	if (Number.isInteger(key))
		key = [key];
	return new Promise(function (resolve, reject) {
		if (Array.isArray(ops) && Array.isArray(key)){
			if (typeof(spaceId)=='string' || typeof(indexId)=='string')
			{
				return this._getMetadata(spaceId, indexId)
					.then(function(info){
						return this.update(info[0], info[1],  key, ops);
					}.bind(this))
					.then(resolve)
					.catch(reject);
			}
			var reqId = this._getRequestId();
			var bufKey = this.msgpack.encode(key);
			var bufOps = this.msgpack.encode(ops);

			var len = 18+bufKey.length+bufOps.length;
			var buffer = utils.createBuffer(len+5);

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
			
			this.sendCommand([tarantoolConstants.RequestCode.rqUpdate, reqId, {resolve: resolve, reject: reject}], buffer);
		}
		else
			reject(new utils.TarantoolError('need array'));
	}.bind(this));
};

Commands.prototype.upsert = function(spaceId, ops, tuple){
	return new Promise(function (resolve, reject) {
		if (Array.isArray(ops)){
			if (typeof(spaceId)=='string')
			{
				return this._getMetadata(spaceId, 0)
					.then(function(info){
						return this.upsert(info[0], ops, tuple);
					}.bind(this))
					.then(resolve)
					.catch(reject);
			}
			var reqId = this._getRequestId();
			var bufTuple = this.msgpack.encode(tuple);
			var bufOps = this.msgpack.encode(ops);

			var len = 16+bufTuple.length+bufOps.length;
			var buffer = utils.createBuffer(len+5);

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

			this.sendCommand([tarantoolConstants.RequestCode.rqUpsert, reqId, {resolve: resolve, reject: reject}], buffer);
		}
		else
			reject(new utils.TarantoolError('need ops array'));
	}.bind(this));
};


Commands.prototype.eval = function(expression){
	var tuple = Array.prototype.slice.call(arguments, 1);
	return new Promise(function (resolve, reject) {
		var reqId = this._getRequestId();
		var bufExp = this.msgpack.encode(expression);
		var bufTuple = this.msgpack.encode(tuple ? tuple : []);
		var len = 15+bufExp.length + bufTuple.length;
		var buffer = utils.createBuffer(len+5);

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

		this.sendCommand([tarantoolConstants.RequestCode.rqEval, reqId, {resolve: resolve, reject: reject}], buffer);
	}.bind(this));
};

Commands.prototype.call = function(functionName){
	var tuple = arguments.length > 1 ? Array.prototype.slice.call(arguments, 1): [];
	return new Promise(function (resolve, reject) {
		var reqId = this._getRequestId();
		var bufName = this.msgpack.encode(functionName);
		var bufTuple = this.msgpack.encode(tuple ? tuple : []);
		var len = 15+bufName.length + bufTuple.length;
		var buffer = utils.createBuffer(len+5);

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

		this.sendCommand([tarantoolConstants.RequestCode.rqCall, reqId, {resolve: resolve, reject: reject}], buffer);
	}.bind(this));
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
	return new Promise(function (resolve, reject) {
		if (Array.isArray(tuple)){
			if (typeof(spaceId)=='string')
			{
				return this._getMetadata(spaceId, 0)
					.then(function(info){
						return this._replaceInsert(cmd, reqId, info[0], tuple);
					}.bind(this))
					.then(resolve)
					.catch(reject);
			}
			var bufTuple = this.msgpack.encode(tuple);
			var len = 15+bufTuple.length;
			var buffer = utils.createBuffer(len+5);

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

			this.sendCommand([cmd, reqId, {resolve: resolve, reject: reject}], buffer);
		}
		else
			reject(new utils.TarantoolError('need array'));
	}.bind(this));
};

Commands.prototype._auth = function(username, password){
	return new Promise(function (resolve, reject) {
		var reqId = this._getRequestId();

		var user = this.msgpack.encode(username);
		var scrambled = scramble(password, this.salt);
		var len = 44+user.length;
		var buffer = utils.createBuffer(len+5);

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

		this.commandsQueue.push([tarantoolConstants.RequestCode.rqAuth, reqId, {resolve: resolve, reject: reject}]);
		this.socket.write(buffer);
	}.bind(this));
};

function shatransform(t){
	return crypto.createHash('sha1').update(t).digest();
}

function xor(a, b) {
	if (!Buffer.isBuffer(a)) a = new Buffer(a);
	if (!Buffer.isBuffer(b)) b = new Buffer(b);
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
	return new Buffer(res);
}

function scramble(password, salt){
	var encSalt = new Buffer(salt, 'base64');
	var step1 = shatransform(password);
	var step2 = shatransform(step1);
	var step3 = shatransform(Buffer.concat([encSalt.slice(0, 20), step2]));
	return xor(step1, step3);
}

module.exports = Commands;