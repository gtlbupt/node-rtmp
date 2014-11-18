
var RTMPServer = require('../').RTMPServer;
var server = new RTMPServer();
var port = 1935;
server.on('connect', function(nc){
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
server.listen(port, function(){
	console.log("RTMPServer listen at port: ", port);
});
