/* global Promise */
'use strict';

var Benchmark = require('benchmark');
var suite = new Benchmark.Suite();
var Driver = require('../lib/connection.js');
var noop = require('lodash/noop');
var promises;
var preparedSelectStmtId;

var connectionArg = process.argv[process.argv.length - 1]

var conn = new Driver(connectionArg, {
	lazyConnect: true,
	tuplesToObjects: true
});

var connAutoPipelined = new Driver(connectionArg, {
	lazyConnect: true,
	autoPipeliningPeriod: 10 // just a 1 millisecond window!
});

Promise.all([
	conn.connect(),
	connAutoPipelined.connect()
])
// preload schema and create a prepared SQL statement
.then(function () {
	return Promise.all([
		conn.selectCb('counter', 0, 1, 0, 'eq', ['test'], noop, noop),
		connAutoPipelined.selectCb('counter', 0, 1, 0, 'eq', ['test'], noop, noop),
		conn.prepare('SELECT * FROM "counter" WHERE "primary" = ? LIMIT 1 OFFSET 0')
		.then(function (result) {
			preparedSelectStmtId = result.id
		})
	])
})
.then(function(){
	// non-deferred benchmarks measures the real performance of code, e.g. not awaiting for the response to be received
	suite.add('non-deferred select', {defer: false, fn: function(){
		conn.selectCb('counter', 0, 1, 0, 'eq', ['test'], noop, console.error);
	}});

	suite.add('non-deferred select, tuplesToObjects', {defer: false, fn: function(){
		conn.selectCb('counter', 0, 1, 0, 'eq', ['test'], noop, console.error, {
			tuplesToObjects: true
		});
	}});

	// show the performance improvement when using an autopipelining
	suite.add('non-deferred select + autopipelining window of 1ms', {defer: false, fn: function(){
		connAutoPipelined.selectCb('counter', 0, 1, 0, 'eq', ['test'], noop, console.error);
	}});

	suite.add('non-deferred sql select', {defer: false, fn: function(){
		conn.sql('SELECT * FROM "counter" WHERE "primary" = ? LIMIT 1 OFFSET 0', ['test']);
	}});

	suite.add('non-deferred sql prepared select', {defer: false, fn: function(){
		conn.sql(preparedSelectStmtId, ['test']);
	}});

	suite.add('select cb', {defer: true, fn: function(defer){
		function callback(){
			defer.resolve();
		}
		conn.selectCb('counter', 0, 1, 0, 'eq', ['test'], callback, console.error);
	}});

	suite.add('select promise', {defer: true, fn: function(defer){
		conn.select('counter', 0, 1, 0, 'eq', ['test'])
			.then(function(){ defer.resolve();});
	}});

	suite.add('paralell 500', {defer: true, fn: function(defer){
		try{
			promises = [];
			for (let l=0;l<500;l++){
				promises.push(conn.select('counter', 0, 1, 0, 'eq', ['test']));
			}
			var chain = Promise.all(promises);
			chain.then(function(){ defer.resolve(); })
				.catch(function(e){
					console.error(e, e.stack);
					defer.reject(e);
				});
		} catch(e){
			defer.reject(e);
			console.error(e, e.stack);
		}
	}});

	suite.add('paralel by 10', {defer: true, fn: function(defer){
		var chain = Promise.resolve();
		try{
			for (var i=0;i<50;i++)
			{
				chain = chain.then(function(){
					promises = [];
					for (var l=0;l<10;l++){
						promises.push(
							conn.select('counter', 0, 1, 0, 'eq', ['test'])
						);
					}
					return Promise.all(promises);
				});
			}

			chain.then(function(){ defer.resolve(); })
				.catch(function(e){
					console.error(e, e.stack);
				});
		} catch(e){
			console.error(e, e.stack);
		}
	}});

	suite.add('paralel by 50', {defer: true, fn: function(defer){
		var chain = Promise.resolve();
		try{
			for (var i=0;i<10;i++)
			{
				chain = chain.then(function(){
					promises = [];
					for (var l=0;l<50;l++){
						promises.push(
							conn.select('counter', 0, 1, 0, 'eq', ['test'])
						);
					}
					return Promise.all(promises);
				});
			}

			chain.then(function(){ defer.resolve(); })
				.catch(function(e){
					console.error(e, e.stack);
				});
		} catch(e){
			console.error(e, e.stack);
		}
	}});

	suite.add('pipelined select by 10', {defer: true, fn: function(defer){
		var pipelinedConn = conn.pipeline()
		
		for (var i=0;i<10;i++) {
			pipelinedConn.select('counter', 0, 1, 0, 'eq', ['test']);
		}

		pipelinedConn.exec()
		.then(function(){ defer.resolve(); })
		.catch(function(e){ defer.reject(e); });
	}});

	suite.add('pipelined select by 50', {defer: true, fn: function(defer){
		var pipelinedConn = conn.pipeline()
		
		for (var i=0;i<50;i++) {
			pipelinedConn.select('counter', 0, 1, 0, 'eq', ['test']);
		}

		pipelinedConn.exec()
		.then(function(){ defer.resolve(); })
		.catch(function(e){ defer.reject(e); });
	}});

	suite
	.on('cycle', function(event) {
		console.log(String(event.target));
	})
	.on('complete', function() {
		console.log('complete');
		process.exit();
	})
	.run({ 'async': true, 'queued': true });
});
