var { Decoder: msgpackDecoder } = require('msgpack-lite');
var tarantoolConstants = require('./const');
var { TarantoolError } = require('./utils');
var { codec } = require('./msgpack-extensions');

var decoder = new msgpackDecoder({codec});

exports._processResponse = function(buffer, offset){
  decoder.buffer = buffer;
  decoder.offset = offset || 0;
  var headers = decoder.fetch();
  var schemaId = headers[tarantoolConstants.KeysCode.schema_version]
  var reqId = headers[tarantoolConstants.KeysCode.sync]
  var code = headers[tarantoolConstants.KeysCode.code]
  var data = decoder.fetch()

  if (this.schemaId)
  {
    if (this.schemaId != schemaId)
    {
      this.schemaId = schemaId;
      this.namespace = {};
    }
  }
  else
  {
    this.schemaId = schemaId;
  }
  var task = this.sentCommands.get(reqId);
  this.sentCommands.delete(reqId);
	var dfd = task[1];
  var timeoutId = task[3];

  if (timeoutId) clearTimeout(timeoutId);

  if (code === 0) {
    dfd[0](this._returnBool(task, data));
  }
	else {
    var tarantoolErrorObject = data[tarantoolConstants.KeysCode.iproto_error] && data[tarantoolConstants.KeysCode.iproto_error][0x00][0]
    var errorDecription = (tarantoolErrorObject && tarantoolErrorObject[0x03]) || data[tarantoolConstants.KeysCode.iproto_error_24]

    if ([
      358 /* code of 'read-only' */, 
      "Can't modify data on a read-only instance - box.cfg.read_only is true",
      3859 /* code of 'bootstrap not finished' */
    ].includes((tarantoolErrorObject && tarantoolErrorObject[0x02]) || errorDecription)) {
      switch (this.options.nonWritableHostPolicy) {
        case 'changeAndRetry':
          var attemptsCount = task[2].attempt
          if (attemptsCount) {
            task[2].attempt++
          } else {
            task[2].attempt = 1
          }

          if (this.options.maxRetriesPerRequest <= attemptsCount) {
            return dfd[1](new TarantoolError(errorDecription));
          }

          this.offlineQueue.push([[task[0], task[1], task[2]], task[3]]);
          return  changeHost.call(this, errorDecription);
        case 'changeHost':
          changeHost.call(this, errorDecription);
      }
    }

    dfd[1](new TarantoolError(errorDecription));
  }
};

function _returnBool(task, data){
  var cmd = task[0];
	switch (cmd){
		case tarantoolConstants.RequestCode.rqAuth:
		case tarantoolConstants.RequestCode.rqPing:
			return true;
    case tarantoolConstants.RequestCode.rqExecute:
      if (data[tarantoolConstants.KeysCode.metadata]) {
        var res = [];
        var meta = data[tarantoolConstants.KeysCode.metadata];
        var rows = data[tarantoolConstants.KeysCode.data];
        for (var i = 0; i < rows.length; i++) {
          var formattedRow = {};
          for (var j = 0; j < meta.length; j++ ) {
            formattedRow[meta[j][0x00]] = rows[i][j];
          }
          res.push(formattedRow);
        }
        return res;
      } else {
        return 'Affected row count: ' + (data[tarantoolConstants.KeysCode.sql_info][0x0] || 0);
      }
    case tarantoolConstants.RequestCode.rqPrepare:
        return {
            id: data[tarantoolConstants.KeysCode.stmt_id],
            metadata: data[tarantoolConstants.KeysCode.metadata],
            bind_metadata: data[tarantoolConstants.KeysCode.bind_metadata]
        };
    case tarantoolConstants.RequestCode.rqId:
      return {
          version: data[tarantoolConstants.KeysCode.iproto_version],
          features: data[tarantoolConstants.KeysCode.iproto_features],
          auth_type: data[tarantoolConstants.KeysCode.iproto_auth_type]
      };
    case tarantoolConstants.RequestCode.rqSelect:
    case tarantoolConstants.RequestCode.rqInsert:
    case tarantoolConstants.RequestCode.rqReplace:
    case tarantoolConstants.RequestCode.rqUpdate:
    case tarantoolConstants.RequestCode.rqDelete:
      if (task[4]) {
        // should load the new space shema in case of change
        if (!this.namespace[task[2][0]]) {
          return [];
        }
        return convertTupleToObject(data[tarantoolConstants.KeysCode.data], this.namespace[task[2][0]].tupleKeys)
      };
    default:
      return data[tarantoolConstants.KeysCode.data];
	}
}
exports._returnBool = _returnBool

function changeHost (errorDecription) {
  this.setState(512, errorDecription) // event 'changing_host'
  this.useNextReserve()
  this.disconnect(true)
}

function convertTupleToObject (rows, tupleKeys) {
  var arr = new Array(rows.length).fill([]);
  var num = 0;
  for (var row of rows) {
    var obj = createObject(tupleKeys, row)
    arr[num] = obj
    num++;
  }

  return arr;
}

function createObject (keys, values) {
  var num = 0;
  var obj = {};
  for (var key of keys) {
    obj[key] = values[num]
    num++
  }

  return obj;
}