'use strict'
var RTMP = require('../');
var port = 1935;
var server = RTMP.createServer({isEdge: false},  function(nc){
	nc.accept();
	nc.on('createStream', function(ns){
		ns.on('publish', function(args, hasPublish){
			console.log("publish args[%s] hasPublish[%d]", JSON.stringify(args), hasPublish);
			ns.acceptPublish(args);	
		});	
		ns.on('play', function(args){
			console.log("publish args[%s] hasPublish[%d]", JSON.stringify(args));
			ns.acceptPlay(args);
		});
	});
});
server.listen(port, '0.0.0.0', 511, function(){
	console.log("RTMPServer listen at port: ", port);
});
