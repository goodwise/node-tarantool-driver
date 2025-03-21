var { KeysCode, RequestCode } = require('./const');
var { TarantoolError } = require('./utils');
var debug = require('debug')('tarantool-driver:parser');

exports.processResponse = function(headers, data){
  var schemaId = headers[KeysCode.schema_version]
  var reqId = headers[KeysCode.sync]
  var code = headers[KeysCode.code]
  debug(`processing response for request №${reqId}; code: ${code}, data: `, data, ', headers: ', headers)

  if (this.schemaId) {
    if (this.schemaId != schemaId) {
      this.schemaId = schemaId;
      this.namespace = {};
    }
  } else {
    this.schemaId = schemaId;
  }

  var task = this.sentCommands.get(reqId);
  this.sentCommands.delete(reqId);
	var dfd = task && task[1];
  var timeoutId = task && task[3];

  if (timeoutId) clearTimeout(timeoutId);

  if (code === 0) {
    dfd[0](this._returnBool(task, data));
  } else {
    var tarantoolErrorObject = data[KeysCode.iproto_error] && data[KeysCode.iproto_error][0x00][0]
    var errCode = tarantoolErrorObject && tarantoolErrorObject[0x05]
    var errDecription = (tarantoolErrorObject && tarantoolErrorObject[0x03]) || data[KeysCode.iproto_error_24]

    switch (errCode) {
      case 7: /* ER_READONLY */
      case 116: /* ER_LOADING */
        switch (this.options.nonWritableHostPolicy) {
          case "changeAndRetry":
            var attemptsCount = task[2].attempt;
            if (attemptsCount) {
              task[2].attempt++;
            } else {
              task[2].attempt = 1;
            }

            if (this.options.maxRetriesPerRequest <= attemptsCount) {
              return dfd[1](new TarantoolError(errDecription));
            }

            this.offlineQueue.push([[task[0], task[1], task[2]], task[3]]);
            return changeHost.call(this, errDecription);
          case "changeHost":
            changeHost.call(this, errDecription);
          default:
            dfd[1](new TarantoolError(errDecription));
        }
      break;
      default:
        if (reqId) return dfd[1](new TarantoolError(errDecription));

        this.errorHandler(
          new TarantoolError(
            "Processed response with an unsuccessful response code: " + errDecription
          )
        );
    }
  }
};

exports._returnBool = async function _returnBool (task, data){
  var cmd = task[0];
	switch (cmd){
		case RequestCode.rqAuth:
		case RequestCode.rqPing:
			return true;
    case RequestCode.rqExecute:
      if (data[KeysCode.metadata]) {
        var res = [];
        var meta = data[KeysCode.metadata];
        var rows = data[KeysCode.data];
        for (var i = 0; i < rows.length; i++) {
          var formattedRow = {};
          for (var j = 0; j < meta.length; j++ ) {
            formattedRow[meta[j][0x00]] = rows[i][j];
          }
          res.push(formattedRow);
        }
        return res;
      } else {
        return 'Affected row count: ' + (data[KeysCode.sql_info][0x0] || 0);
      }
    case RequestCode.rqPrepare:
        return data[KeysCode.stmt_id];
    case RequestCode.rqId:
      return {
          version: data[KeysCode.iproto_version],
          features: data[KeysCode.iproto_features],
          auth_type: data[KeysCode.iproto_auth_type]
      };
    case RequestCode.rqSelect:
    case RequestCode.rqInsert:
    case RequestCode.rqReplace:
    case RequestCode.rqUpdate:
    case RequestCode.rqDelete:
      if (task[4]) {
        // should load the new space shema in case of change
        if (!this.namespace[task[2][0]]) {
          return [];

        }
        return convertTupleToObject(data[KeysCode.data], this.namespace[task[2][0]].tupleKeys)
      };
    default:
      return data[KeysCode.data];
	}
}

function changeHost (errDecription) {
  this.setState(512, errDecription) // event 'changing_host'
  this.useNextReserve()
  this.disconnect(true)
}

function convertTupleToObject (rows, tupleKeys) {
  return rows.map(function (row) {
    return createObject(tupleKeys, row);
  })
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