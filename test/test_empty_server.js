'use strict'
var RTMP = require('../');
console.dir(RTMP);
var port = 1935;
var server = RTMP.createServer(null,  function(nc){
	nc.accept();
	nc.on('createStream', function(ns){
		console.log("createStream");
		ns.on('publish', function(){
			console.log("publish");
			ns.acceptPublish();	
		});	
		ns.on('play', function(){
		});
	});
});
server.listen(port, '0.0.0.0', 511, function(){
	console.log("RTMPServer listen at port: ", port);
});
