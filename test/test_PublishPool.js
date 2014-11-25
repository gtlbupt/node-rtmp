'use strict'
var RTMP = require('../');

var server = RTMP.createServer(null, function(nc){});

var pool = new RTMP.PublishPool({server: server});

var req = {vhost: '', app: 'live', streamName: 'livestream'}; 
pool.emit('publish', req, {});
pool.emit('play', req, new RTMP.NetStream({}));
//pool.emit('stopPublish', req, {});
