// i steal it from go
var RequestCode = {
	rqConnect: 0x00, //fake for connect
	rqSelect: 0x01,
	rqInsert: 0x02,
	rqReplace: 0x03,
	rqUpdate: 0x04,
	rqDelete: 0x05,
	rqCall: 0x06,
	rqAuth: 0x07,
	rqEval: 0x08,
	rqUpsert: 0x09,
	rqRollback: 0x10,
	rqCallNew: 0x0a,
	rqExecute: 0x0b,
	rqPrepare: 0x0d,
	rqBegin: 0x0e,
	rqCommit: 0x0f,
	rqDestroy: 0x100, //fake for destroy socket cmd
	rqPing: 0x40,
	rqId: 0x49
};

var KeysCode = {
	code: 0x00,
	sync: 0x01,
	schema_version: 0x05,
	space_id: 0x10,
	index_id: 0x11,
	limit: 0x12,
	offset: 0x13,
	iterator: 0x14,
	key: 0x20,
	tuple: 0x21,
	function_name: 0x22,
	username: 0x23,
	expression: 0x27,
	def_tuple: 0x28,
	data: 0x30,
	iproto_error_24: 0x31,
	metadata: 0x32,
	bind_metadata: 0x33,
	sql_text: 0x40,
	sql_bind: 0x41,
	sql_info: 0x42,
	stmt_id: 0x43,
	iproto_error: 0x52,
	iproto_version: 0x54,
	iproto_features: 0x55,
	iproto_timeout: 0x56,
	iproto_txn_isolation: 0x59,
	iproto_stream_id: 0x0a,
	iproto_auth_type: 0x5b
};

// https://github.com/fl00r/go-tarantool-1.6/issues/2
var IteratorsType = {
	eq: 0,
	req: 1,
	all: 2,
	lt: 3,
	le: 4,
	ge: 5,
	gt: 6,
	bitsAllSet: 7,
	bitsAnySet: 8,
	bitsAllNotSet: 9
};

var OkCode            = 0;
var NetErrCode        = 0xfffffff1;  // fake code to wrap network problems into response
var TimeoutErrCode    = 0xfffffff2;  // fake code to wrap timeout error into repsonse

var Space = {
	schema: 272,
	space: 281,
	index: 289,
	func: 296,
	user: 304,
	priv: 312,
	cluster: 320
};

var IndexSpace = {
	primary: 0,
	name: 2,
	indexPrimary: 0,
	indexName: 2
};

const revertStates = {
    0: 'connecting',
    1: 'connected',
    2: 'awaiting',
    4: 'inited',
    8: 'prehello',
    16: 'awaiting_length',
    32: 'end',
    64: 'reconnecting',
    128: 'auth',
    256: 'connect',
    512: 'changing_host'
};

const states = {
    CONNECTING: 0,
    CONNECTED: 1,
    AWAITING: 2,
    INITED: 4,
    PREHELLO: 8,
    AWAITING_LENGTH: 16,
    END: 32,
    RECONNECTING: 64,
    AUTH: 128,
    CONNECT: 256,
    CHANGING_HOST: 512
};

module.exports = {
	states,
	revertStates,
	RequestCode,
	KeysCode,
	IteratorsType,
	OkCode,
	passEnter: Buffer.from('a9636861702d73686131', 'hex') /* from msgpack.encode('chap-sha1') */,
	Space,
	IndexSpace
};