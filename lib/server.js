/**
 * Module dependencies.
 */

var assert          = require('assert');
var net             = require('net');
var path            = require('path');
var fs              = require('fs');
var inherits        = require('util').inherits;
var Writable        = require('stream').Writable;
var Parser          = require('stream-parser');
var debug           = require('debug')('rtmp');
var amf             = require('amf');
var handshake       = require('./handshake.js');
var Encoder         = require('./encoder.js');
var amf0Types       = amf.amf0Types;

// node v0.8.x compat
if (!Writable) Writable = require('readable-stream/writable');

/**
 * Constants Definition!.
 **/

const CONSTS = {
    outChunkSize: 60000,
};



/**
 * Module exports.
 */

module.exports.TsTimeJitter     = TsTimeJitter;
module.exports.NetConnection    = NetConnection;
module.exports.NetStream        = NetStream;
module.exports.EdgePublish		= EdgePublish;
module.exports.EdgePlay			= EdgePlay;
module.exports.RTMPServer       = RTMPServer;     
module.exports.createServer     = createServer;

/**
 * class TimeJitter
 * @brief: becase when FMLE or XSplit use csid = 4 send  both audio and video,
 * so the timestamp_delta is the delta to previuse sended packet's time,
 * not the play-duration of packet. so it need we to fix it.
 */
function TsTimeJitter(opts)
{
    if (!(this instanceof TsTimeJitter)){
        return new TsTimeJitter(opts);
    }
    this.audioDuration = 0;
    this.videoDuration = 0;

    this.duration = 0;
    this.lastAudioDuration = 0;
    this.lastVideoDuration = 0;

    this.audioCSID = -1;
    this.videoCSID = -1;
}

/**
 * Jitter Algorithm
 * if audio csid == video csid
 *     total_duration = video.timestamp_delta + audio.timestamp_delta;
 *     msg.timestamp = total_duraion - last_same_type_packet_duration
 *     msg.duration = total_duration;
 * else 
 *    audio_duration = sum(audio.timestamp_delta);
 *    video_duration = sum(video.timestamp_delta);
 *
 *  We always use different csid to seperate send VideoMessage  and video.
 */
TsTimeJitter.prototype.jitter = function(msg){
    switch(msg.type) {
        case 8:
            this.audioCSID = +msg.channelId;
            if (this.audioCSID !== -1  && this.videoCSID !== -1){
                this.isSameCSID = (this.audioCSID == this.videoCSID) ? true: false;
                if (this.isSameCSID){
                    this.duration += msg.timestamp;
                    msg.timestamp = this.duration - this.lastAudioDuration;
                    msg.duration  = this.lastAudioDuration = this.duration;
                } else {
                    this.audioDuration += msg.timestamp;
                    msg.duraton = this.audioDuration;
                }
            }
            break;
        case 9:
            this.videoCSID = +msg.channelId;
            if (this.audioCSID !== -1  && this.videoCSID !== -1){
                this.isSameCSID = (this.audioCSID == this.videoCSID) ? true: false;
                if (this.isSameCSID){
                    this.duration += msg.timestamp;
                    msg.timestamp  = this.duration - this.lastVideoDuration;
                    msg.duration   = this.lastVideoDuration = this.duration;
                } else {
                    this.videoDuration += msg.timestamp;
                    msg.duration  = this.videoDuration;
                }
            }
            break;
        default:
            break;
    }
};

/**
 * NodeJS Thread
 */
function Thread(opts)
{
	if (!(this instanceof Thread)) return new Thread(opts);

	opts || (opts = {});

	this.interval = opts.interval || 10;

	this.index      = 0;
	this.taskLength = opts.taskLength || 64;
	this.slotLength = opts.slotLength || 128;
	this.slots      = new Array(this.slotLength);
	for (var i = 0; i < this.slotLength; i++) {
		this.slots[i] = []; 
	}
}

Thread.prototype.pushTask = function (task){
	console.log("Function[Thread.pushTask] enter");
	if (this.slots[this.index].length >= this.taskLength) { // 64
		this.index = (this.index + 1) % this.slotLength;    // 128
	}
	task._slotIndex = this.index;
	task._taskIndex = this.slots[this.index].length;
	this.slots[this.index].push(task);
	console.log("Function[Thread.pushTask] leave");
};

Thread.prototype.removeTask = function (task){
	console.log("Function[Thread.removeTask]");
    if(!task._slotIndex) {
        return; 
    }
	var slot = this.slots[task._slotIndex];
	for(var i = 0; i < slot.length; i++) {
		if (slot[i] === task) {
			slot.splice(i, 1); 
			break;
		} 
	}
};

Thread.prototype.loop = function (){
    debug("Function[Thread.loop] enter");
    var self = this;
	for(var i = 0; i < this.slotLength; i++) {
		var slot = this.slots[i];
		slot.forEach(function(task){
			task && task.onCycle && task.onCycle(self.interval);
		});
	}
    debug("Function[Thread.loop] leave");
};

Thread.prototype.start = function(){
	this.timer = setInterval(Thread.prototype.loop.bind(this), this.interval);
};


function createServer(opt, callback){
    var server = new RTMPServer(opt);
    server.on('connect', callback);
    return server;
}

/**
 * @brief   : Create a RTMPServer
 * @param	: {Object} opts
 * @port	: {Number} port RTMP listen
 * @maxConn	: {Number} Maximum RTMP Connection 
 * @activeCheckInterval: {Number|ms} time interval to send ping packet. default 0ms not check.
 * @isEdge	: {Boolean} whether to delivery RTMP publish stream to RTMP-Origin-Server. default false;
 *
 * @member: port                {Number}
 * @member: maxConn             {Number} MaxConnection.
 * @member: activeCheckInterval {Number}
 * @member: activeTimer			{Timer}
 * @member: isEdge				{Boolean}
 * @member: sessions			{Object} all NetConnection, index by it's clientId.
 * @member: sessionCount		{Number} The number of sessionCount.
 * @member: defaultChunkSize	{Number} The max chunk size.
 */

function RTMPServer(opts_){
    if( !(this instanceof RTMPServer)){
        return new RTMPServer(opts_); 
    }
    var opts = opts_ || {port:1935};
    this.port = opts.port;
	this.maxConn = opts.maxConn || 3000;
	this.activeCheckInterval	= opts.activeCheckInterval || 0;
	this.activeTimer = null;
	this.isEdge = opts.isEdge || false;

    var _this = this;
	var self  = this;
    this.publishPool   = new PublishPool({server: self});

    this.sessions = {};
    this.sessionCount = 0;
    this.defaultChunkSize   = 65535;

    this.thread = new Thread();
    this._cycleInterval = 0;

    this.createServer();
};
inherits(RTMPServer, require('events').EventEmitter);

RTMPServer.prototype.LOG_LEVEL = {
	kDebug	: 0,
	kInfo	: 1,
	kWarn	: 2,
	kError	: 3,
	kFatal	: 4,
};

RTMPServer.prototype.CODE = {
	kMaxConn	: 1,
};

/**
 * @brief: start RTMPServer to listen
 * @param: port {Number|Optional} 
 */
RTMPServer.prototype.listen = function(port, host, backlog, callback){
	// start ping if this.activeCheckInterval > 0
	if(this.activeCheckInterval > 0){
		//this.startActiveInterval();	
	}

    this.thread.pushTask(this);
    this.thread.start();

    this.port = port || this.port || 1935;
    return this.server.listen(this.port, host, backlog, callback);
};
/**
 * @brief: alias of listen function
 */
RTMPServer.prototype.start = RTMPServer.prototype.listen;

/**
 * @brief: stop the RTMPServer
 * @param: cb {Function}
 */
RTMPServer.prototype.close = function(callback){
	if(this.activeTimer){
		clearInterval(this.activeTimer);	
		this.activeTimer = null;
	}
	return this.server.close(callback); 
};

/**
 * @brief: start a intervalTimer to send ping-pong packet.
 * @param: interval_ {Number| millicons} 
 */
RTMPServer.prototype.startActiveInterval = function (interval_){
	var self = this;
	var interval = interval_ || this.activeCheckInterval;
	this.activeTimer = setInterval(function(){
			for(var key in self.sessions){
				var nc = self.sessions[key];	
				nc.ping();
				debug("ActiveCheckInterval ", nc.stat());
			}
	}, interval);
};

/**
 * @brief: send ping packet 
 */
RTMPServer.prototype.onCycle = function (interval){
    this._cycleInterval += interval;
    if (this.activeCheckInterval > 0 && (this._cycleInterval > this.activeCheckInterval)){
        this._cycleInterval = 0;

        for(var key in this.sessions){
            var nc = this.sessions[key];	
            nc.ping();
            debug("ActiveCheckInterval ", nc.stat());
        }
    }
};

/**
 * Add NetConnection Instance ot RTMPServer.sessions
 */
RTMPServer.prototype.onNetConnectionConnect = function(netConn, args){
	console.log("Function[RTMPServer.onNetConnectionConnect] called, Enter, session count: %d", this.sessionCount);
    // 1. Add sessions
	this.sessions[netConn.clientId] = netConn; 
	this.sessionCount++;

    // 2. Notify User
    if (netConn.role == NetConnection.Role.IN) {
        this.emit('connect', netConn, args); 
    }
	console.log("Function[RTMPServer.onNetConnectionConnect] called, Leave, session count: %d", this.sessionCount);
};

/**
 * Remove NetConn Instance from RTMPServer's sessions
 */
RTMPServer.prototype.onNetConnectionClose = function(netConn){
	console.log("Function[RTMPServer.onNetConnectionClose] called, Enter, session count: %d", this.sessionCount);
	if(netConn.clientId in this.sessions){
		this.sessionCount--;
		delete this.sessions[netConn.clientId];
	}
	console.log("Function[RTMPServer.onNetConnectionClose] called, leave, session count: %d", this.sessionCount);
};

RTMPServer.prototype.createServer = function(){
    // return net.server;
    var self = this;
    this.server =  net.createServer(function(socket){
			if(self.sessionCount >= self.maxConn){
				debug("session reach maxConns: %d, reject the new connection", self.maxConn);
				self.emit("error", {code: self.CODE.kMaxConn , level: self.LEVEL.kError, description: "reach max connections"});	
				socket.end();
				return;
			}

            var opts = {_server: self, _socket: socket, _role: NetConnection.Role.IN};
            var netConn = new NetConnection(opts);

            netConn.on('connect', function(args){
                debug('NetConnection[%s] connect',netConn.clientId);
                self.onNetConnectionConnect(netConn, args); 
            });
            netConn.on('close', function(err){
                debug('NetConnection[%s] close, err',netConn.clientId, err);
                self.onNetConnectionClose(netConn); 
            });
    });
};

/*-----------------PublishPool-----------------------------------------
 *
 * @brief: put stream in PublishPool.
 *   play stream must subscrib.
 **/

/**
 * Pipe
 * @member state		{enum: INIT|PUBLISH}
 * @member publisher	{NetStream}
 * @member subscribers	{Array}
 * @member metadata		{Message|Object}
 * @member aacSeqHdr	{Message|Object}
 * @member avcSeqHdr	{Message|Object}
 * @member msgQueue		{MessageQueue}
 */

function Pipe(opts){
	if (!(this instanceof Pipe)){
		return new Pipe(opts);
	}
	this.state			= Pipe.STATE.INIT;

	this.publisher		= opts.publisher;
	this.subscribers	= [];

    this.source_id      = null;
    this.hls            = null; // hls handler
    this.dvr            = null; // dvr handler
    this.encoder        = null; // transcoding handler
    this.edgePlay       = null; // edge control service
    this.edgePublish    = null; // edge control service
    // this.timeJitter     = new TsTimeJitter();
	this.appendingQueue	= [];

	this.metadata	= null;
	this.aacSeqHdr	= null;
	this.avcSeqHdr	= null;
	this.msgQueue	= new MessageQueue({duration:10*1000});
	this.pubSize	= 0;
	this.subSize	= 0;

	this.vhost	= opts.vhost;
	this.app	= opts.app;
	this.streamName = opts.streamName;
	this.on('publish', Pipe.prototype.onPublish.bind(this));
	this.on('unpublish', Pipe.prototype.onUnpublish.bind(this));
	this.on('play', Pipe.prototype.onPlay.bind(this));
	this.on('stopPlay', Pipe.prototype.onStopPlay.bind(this));
}
inherits(Pipe, require('events').EventEmitter);

Pipe.STATE = {
	INIT		: 0x01,
	PUBLISH		: 0x02,
	STOP_PUBLISH: 0x04,
	PLAY		: 0x08,
	STOP_PLAY	: 0x10,
	PULL_PENDING: 0x20,
	PUSH_PENDING: 0x40,	
};


Pipe.prototype.onMetaData = function(metadata){
	console.log("Function[Pipe.onMetaData] enter");
	this.metadata = metadata;
	this.subscribers.forEach(function(stream){
		stream.sendMetadata(metadata);
	});
	console.log("Function[Pipe.onMetaData] leave");
};

Pipe.prototype.onAacSeqHdr = function (aacSeqHdr){
	console.log("Function[Pipe.onAacSeqHdr] enter");
	this.aacSeqHdr = aacSeqHdr;
	this.subscribers.forEach(function(stream){
		stream.sendAudio(aacSeqHdr);	
	});
	console.log("Function[Pipe.onAacSeqHdr] leave");
};

Pipe.prototype.onAvcSeqHdr = function (avcSeqHdr){
	console.log("Function[Pipe.onAvcSeqHdr] enter");
	this.avcSeqHdr = avcSeqHdr;
	this.subscribers.forEach(function(stream){
		stream.sendVideo(avcSeqHdr);
	});
	console.log("Function[Pipe.onAvcSeqHdr] leave");
};

Pipe.prototype.onAudio = function(msg){
	//console.log("Function[Pipe.onAudio] enter, subSize: %d ", this.subscribers.length);
    //
    // this.timeJitter && this.timeJitter.jitter(msg); 

	this.msgQueue.push(msg);
	this.subscribers.forEach(function(stream){
		stream.recvAudio(msg);	
	});
	//console.log("Function[Pipe.onAudio] leave, subSize: %d ", this.subscribers.length);
};

Pipe.prototype.onVideo = function (msg){
	// console.log("Function[Pipe.onVideo] enter, subSize: %d, msgQueueSize: %d", this.subscribers.length, this.msgQueue.size());
    // this.timeJitter && this.timeJitter.jitter(msg); 

	this.msgQueue.push(msg);
	this.subscribers.forEach(function(stream){
		stream.recvVideo(msg);
	});
	// console.log("Function[Pipe.onVideo] leave, subSize: %d", this.subscribers.length);
};

Pipe.prototype.switchPublisher = function(stream)
{
	if(this.state == Pipe.STATE.PULL_PENDING){
		this.state = Pipe.STATE.PUBLISH;	
	}
	if (this.publisher){
		this.publisher.emit('switchPublisher');	
	}
	this.publisher = stream;
}

Pipe.prototype.onPublish = function (req, stream){
	console.log("Function[Pipe.onPublish] enter");
    stream.pipe = this;

	var self = this;
	this.publisher = stream;
	this.pubSize++;
	if (this.state == Pipe.STATE.INIT){
		console.log("Function[Pipe.onPublish] state: %d, pendingQueue length: %d", this.state, this.appendingQueue.length);
		this.appendingQueue.forEach(function(stream){
			self.appendSubscriber(stream);
		});	
		this.appendingQueue = [];
		console.log("Function[Pipe.onPublish] state: %d, pendingQueue length: %d", this.state, this.appendingQueue.length);
	}
	this.state = Pipe.STATE.PUBLISH;

	console.log("Function[Pipe.onPublish] leave, state: %d", this.state);
};

Pipe.prototype.onUnpublish = function(req, stream){
	debug("Function[Pipe.onUnpublish] enter, state: %d", this.state);
	console.log("Function[Pipe.onUnpublish] enter, state: %d, subSize: %d", this.state, this.subscribers.length);
	//assert(this.state === Pipe.STATE.PUBLISH);
	this.state = Pipe.STATE.STOP_PUBLISH;
	this.subscribers.forEach(function(stream){
		stream.stopPublish(function(){
            console.log("stopPublish callback");
            stream.netConn.close(); 
        });
	});
	this.publisher = null;
	this.pubSize--;
	console.log("Function[Pipe.onUnpublish] leave, state: %d, subSize: %d", this.state, this.subscribers.length);
}

Pipe.prototype.onPlay = function(stream){
	console.log("Function[Pipe.onPlay] enter");
	if (this.state == Pipe.STATE.INIT){
		this.appendingQueue.push(stream);	
	}else {
	    console.log("Function[Pipe.onPlay] append subscribe");
		this.appendSubscriber(stream);
	}
	console.log("Function[Pipe.onPlay] leave");
};

Pipe.prototype.onStopPlay = function (stream){
	console.log("Function[Pipe.onStopPlay]");
	this.removeSubscriber(null, stream);
};

Pipe.prototype.onEdgeStartPlay = function (){};

Pipe.prototype.onEdgeStart_publish = function(){};

Pipe.prototype.onClose = function (req, stream){
	console.log("Function[Pipe.onClose] ", this.subscribers.length);
	/**
	this.subscribers.forEach(function(stream){
		stream.emit('unPublish');	
	});	
	**/
	this.state		 = Pipe.STATE.CLOSE;
	this.publisher	 = null;
	this.subscribers = [];
	this.metadata	 = null;
	this.aacSeqHdr	 = null;
	this.avcSeqHdr	 = null;
	this.msgQueue	 = null;
	console.log("Function[Pipe.onClose] ", this.subscribers.length);
};

Pipe.prototype.removeSubscriber = function (req, stream){
	console.log("Function[removeSubscriber] called, sub number: %d", this.subscribers.length);
	for(var i = 0; i < this.subscribers.length; i++){
		if ( this.subscribers[i] === stream) {
			this.subscribers.splice(i,1);	
			this.subSize--;
			break;
		}
	}
	console.log("Function[removeSubscriber] called, sub number: %d", this.subscribers.length);
}

Pipe.prototype.appendSubscriber = function (stream){
	console.log("Function[Pipe.appendSubscriber] enter, susbscriber number: %s", this.subscribers.length);
	this.subscribers.push(stream);
	this.subSize++;

	if (this.metadata) {
		console.log("Function[Pipe.appendSubscriber write metadata");
        stream.sendMetadata(this.metadata); 
	}
	if (this.aacSeqHdr) {
		console.log("Function[Pipe.appendSubscriber write AacSeqHdr");
		stream.sendAudio(this.aacSeqHdr);	
	}
	if (this.avcSeqHdr) {
		console.log("Function[Pipe.appendSubscriber write AvcSeqHdr");
		stream.sendVideo(this.avcSeqHdr);	
	}

	this.msgQueue.forEach(function(Msg){
		stream.msgQueue.push(Msg);	
	});

    stream.waitForKeyFrame = true;

    stream.startCycle();
    return;

    var self = stream;
    stream.timerId = setInterval(function(){
		var count = 0;
		while(self.msgQueue.length > 0 && count++ < 5){
			var msg = self.msgQueue.shift();

            if (stream.state == NetStream.STATE.STOP_PLAY){
                continue; 
            }

			if(stream.waitForKeyFrame === true){
				if(msg.type !== 0x09){
					return; 
				}
				if(!msg.isKeyFrame){
					return; 
				}
				stream.waitForKeyFrame = false; 
			}
			if(msg.type === 0x08){
				self.sendAudio(msg); 
			}else if(msg.type === 0x09){
				self.sendVideo(msg);
			}else {
			}
		}
    },
    50);

	console.log("Function[Pipe.appendSubscriber] leave, susbscriber number: %s", this.subscribers.length);
};

function PublishPool(opts){
	if(!(this instanceof PublishPool)){
		return new PublishPool(opts);
	}

	this.server = opts.server;
    this.pool = {};
	this.size = 0;

	this.on('publish', PublishPool.prototype.onPublish.bind(this));
	this.on('unpublish', PublishPool.prototype.onUnpublish.bind(this));
	this.on('onStatus', PublishPool.prototype.onStatus.bind(this));
	this.on('closeStream', PublishPool.prototype.onCloseStream.bind(this));
}
inherits(PublishPool, require('events').EventEmitter);

PublishPool.prototype.onPublish = function (req, stream){
	console.log("PublishPool onPublish enter, req %s, size: %d, isEdge: %d", JSON.stringify(req), this.size, this.server.isEdge);
	var pipe = new Pipe({vhost: stream.vhost, app: stream.app, streamName: stream.streamName});
	Pipe.prototype.onPublish.call(pipe, null, stream);
	this.set(req, pipe);
	console.log("PublishPool onPublish leave, req %s, size: %d", JSON.stringify(req), this.size);
	if (this.server.isEdge){
		pipe.edgePublisher = new EdgePublish({server: this.server, pipe: pipe});
		pipe.edgePublisher.connect({
			   host: 'cp01-wise-2011q4ecom05.cp01.baidu.com',
			   port: 8935,
		});
	}
};

/**
 * pass unpublishh message to pipe
 * remove Pipe() int publish pool
 */

PublishPool.prototype.onUnpublish = function (req, stream){
	console.log("Function[PublishPool.onUnpublish] enter, req %s, size: %d", JSON.stringify(req), this.size);
	var pipe = this.get(req);
	if (pipe){ // forward to Pipe
		Pipe.prototype.onUnpublish.apply(pipe, [req, stream]);
		this.del(req);
	}
	console.log("Function[PublishPool.onUnpublish] leave, req %s, size: %d", JSON.stringify(req), this.size);
};

PublishPool.prototype.onStatus  = function(req, stream, args){
	console.log("PublishPool onStatus,req: %j, args", JSON.stringify(req), JSON.stringify(args));
};

PublishPool.prototype.onCloseStream = function(req, stream, isPublisher){
	console.log("Function[PublishPool.onCloseStream] remove stream when stream stop publish");
	var pipe = this.get(req);
	if (pipe){
		if (isPublisher){
			pipe.onUnpublish(req, stream, isPublisher);
			this.del(req);
		} else {
			pipe.removeSubscriber(req, stream);
		}
	}
};

/**
 * @brief: put Publish NetStream in PublishPool.
 * @param[in] req.vhost		{string} VirtualHost
 * @param[in] req.app			{String} Application
 * @param[in] req.streamName	{String}
 * @param[in] req.netStream	{NetStream} Publish NetStream
 * @param[in] Pipe();
 */

PublishPool.prototype.set = function(req, pipe){
	console.log("Function[PublishPool.set] enter");
    var key = path.join(req.vhost, req.app, req.streamName);
    if(key in this.pool){
       throw new Error(key + " already in PublishPool"); 
    }
    //this.pool[key] = req.netStream;
    this.pool[key] = pipe;
	this.size++;

	//pipe.publisher.pipe = pipe;
	console.log("Function[PublishPool.set] leave");
};

PublishPool.prototype.add = PublishPool.prototype.set;

/**
 * @brief: get Publish NetStream in PublishPool.
 * @param[in]: req.vhost
 * @param[in]: req.app
 * @param[in]: req.streamName
 * @param[out]: NetStream if found in NetStream, null if not found
 */
PublishPool.prototype.get = function(req){
    var key = path.join(req.vhost, req.app, req.streamName);
    if(key in this.pool){
       return this.pool[key];
    }
    return null;
}
/**
 * @brief: get Publish NetStream in PublishPool.
 * @param[in]: req.vhost
 * @param[in]: req.app
 * @param[in]: req.streamName
 * @param[out]: true if found in NetStream, false if not found.
 */
PublishPool.prototype.isExist = function(req){
    var key = path.join(req.vhost, req.app, req.streamName);
    if(key in this.pool){
        return true;
    }
    return false;
}

PublishPool.prototype.has		= PublishPool.prototype.isExist;
PublishPool.prototype.contains	= PublishPool.prototype.isExist;

/**
 * @brief: del Publish NetStream in PublishPool.
 * @param[in]: req.vhost
 * @param[in]: req.app
 * @param[in]: req.streamName
 * @param[out]: NetStream if found in NetStream, null if not found
 */
PublishPool.prototype.del = function(req){
    var key = path.join(req.vhost, req.app, req.streamName);
    if(key in this.pool){
       delete this.pool[key];
	   this.size--;
	   return true;
    }
	return false;
}

PublishPool.prototype.remove = PublishPool.prototype.delete = PublishPool.prototype.del;

function ChunkStream(opts)
{
    if (!(this instanceof ChunkStream)) return new ChunkStream(opts);

    this.count = 0;
    this.fmt = 0;
    this.csid = 0;
    this.length = 0;
    this.chunkReadSize = 0; 
    this.msgLength = 0;
    this.msgType = 0;
    this.streamid = 0;
    this.timestamp = 0;
    this.absTimestamp = 0;
    this.msg = null;
}

/**
 * RTMP Message:
 * @param[in] {Number}  fmt	: chunkStream's fmt, can be 0,1,2 or 3.
 * @param[in] {Number}	csid: chunkStream's id, 0, 1 means more byte, 2 => chunk control, >2 for RTMP Message Usage.
 * @param[in] {Number}	type: 8=>audio, 9=>video, 18=>metadata.
 * @param[in] {Number}	timestamp: timestamp delta to previous tag
 * @param[in] {Number}	absTimestamp: absolute timestamp;
 * @param[in] {Boolean} isKeyFrame: whether it is a key frame,only for video(type=9)
 * @param[in] {Buffer:<uint8_t>}	rawData: raw audio or video data;
 * @return Message Instance
 */
function Message(opts){
    if(!(this instanceof Message)) return new Message(opts);

	// chunkStream
    this.fmt            = opts.fmt;
    this.csid           = opts.csid || null;

	// MessageHeader
    this.timestamp      = opts.timestamp || 0;
    this.length         = opts.length;
    this.type           = opts.type;
	this.streamId		= opts.streamId || 0;
    this.absTimestamp   = opts.absTimestamp || 0;

	// MessageBody
    this.isKeyFrame     = opts.isKeyFrame || false;
    this.rawData        = opts.rawData;
    this.body           = opts.body;

	this.size			= opts.length;
	this.duration		= 0;
	this.data			= [opts.body];
}

Message.prototype.isAudio = function(){
	return this.type == 0x08;
};

Message.prototype.isVideo = function(){
	return this.type == 0x09;
};

/**
 * Rtmp MessageQueue
 * @param[in] duration: the max cache time, in ms
 * @member: av_start_time, 
 * @member: av_end_time,
 * @member: queue_size_ms, the duration of cached rtmp message
 * #member: queue , array for [Message]
 **/
function MessageQueue(opts_){
    if(!(this instanceof MessageQueue)) return new MessageQueue(opts_);

    var opts = opts_ || {duration:10*1000};

    this.av_start_time = 0;
    this.av_end_time = 0;
    this.queue_size_ms  = opts.duration || 10*1000;
	this.queue_size_ms  = 10*1000;
    this.queue  = [];

}

MessageQueue.prototype.size = function(){
    return this.queue.length;
};

MessageQueue.prototype.forEach = function (f){
	this.queue.forEach(f);
};

MessageQueue.prototype.push = function(msg){
	this.av_end_time = msg.duration;
	if(this.av_start_time === 0){
		this.av_start_time = msg.duration;	
	}

    this.queue.push(msg);

	while ((this.av_end_time - this.av_start_time) > this.queue_size_ms){
		this.shrink();
	}
};

MessageQueue.prototype.shift = function(msg){
    if(this.quque.length > 0){
		var msg = this.queue.shift();
		this.av_start_time = msg.duration;
        return  msg;
    }else {
        return null; 
    }
}

MessageQueue.prototype.setQueueSize = function(size){
	this.queue_size_ms = size;
}

MessageQueue.prototype.shrink = function(){
	var iframe_idx = -1;
	for(var i = 0; i < this.queue.length; i++){
		if (this.queue[i].isKeyFrame){
			iframe_idx = i;	
			this.av_start_time = this.queue[i].duration;
			break;
		}
	}

	if (iframe_idx < 0) {
		this.queue = [];
		this.av_start_time = this.av_end_time = 0;
		return;
	}

	this.queue.splice(0, iframe_idx+1); 
};


/**
 * @brief: Represent a underly channel that can transport NetStream.
 * @param[in]: opts._server	{RTMPServer|optional}
 * @param[in]: opts._socket	{net.socket|optional}
 *
 * @member: time	{Number}
 * @member: state	{Enum}
 * @member: nextTransId		{Number}
 * @member: nextStreamId	{Number}
 * @member: netStreams		{Object} streamId => NetStream HashMap;
 * @member: netStreamCount	{Number}
 * @member: encoder			{Encoder} For Peer. encoder raw message to underlay packet.
 * @member: subscribeEncoder		{Encoder} For subscriber. 
 * @member: server			{RTMPServer}
 * @member: socket			{net.socket} 
 * @member: sendQueu		{Array} 
 * @member: vhost			{String} For NetConnection connect RTMPServer as client.
 * @member: host			{String} For NetConnection connect RTMPServer as client.
 * @member: port			{Number} For NetConnection connect RTMPServer as client.
 * @member: app				{String} For NetConnection connect RTMPServer as client.
 * @member: stub			{Object} For RPC. transId => Function.
 * @member: chunkStreams	{Object} For decode RTMP ChunkStream. csid => chunkStream.
 * @member: enterCount		{Number} For debug
 * @member: leaveCount		{Number} For debug
 * @member: clientId		{String} unique ID
 */
const RTMP_STATE_HANDSHAKE = 0x01;
const RTMP_STATE_CONNECTED = 0x02;
const RTMP_STATE_RUNNING   = 0x04;
const RTMP_STATE_CLOSED    = 0x08;

function NetConnection(opts) {
  console.log("New NetConnection");
  if (!(this instanceof NetConnection)){
      return new NetConnection(opts);
  }
  Writable.call(this, opts);

  this.once('finish', this._onfinish);
    
  if(!opts || !opts.mocha){
    this._bytes(1, this._onc0);
  }

  this.time     = 0;
  this.state    = 0;
  this.state    |= RTMP_STATE_HANDSHAKE;
  this.nextTransId  = 1;
  this.nextStreamId = 1;

  this.netStreams = {};
  this.NetStreamCount = 0;

  // encoder for Peer
  this.encoder  = new Encoder();

  // encoder for subscriber
  this.subscribeEncoder  = new Encoder({chunkSize: CONSTS.outChunkSize});

  this.server = opts._server || null;
  this.socket = opts._socket || null;
  this.role	  = opts._role || NetConnection.Role.IN;
  this.sendQueue    = [];
  this.canSend      = true;


  this.vhost = '';
  this.host  = '127.0.0.1';
  this.port	 = 1935;
  this.app = 'live';

  this.on('connect', NetConnection.prototype.onConnect.bind(this));
  this.on('close', NetConnection.prototype.onClose.bind(this));
  this.on('error', NetConnection.prototype.onError.bind(this));

  var self = this;

  // stream parse pipe
  if(this.socket){
    this.socket.pipe(this);
    this.socket.on('close', function(err){
        self.emit('close', err);
    });
    this.socket.on('error', function(err){
        self.emit('error', err);         
    });
  }

  this.on('sendone',function send(){
    if(self.canSend && self.sendQueue.length > 0){
        var c = self.sendQueue.shift();
        self.canSend = self.socket.write(c, function(){
            self.canSend = true; 
            self.emit('sendone');
        });
        debug('Function[%s] write length: %d, return %s', arguments.callee.name, c.length, self.canSend ? "true" : "false");
    }
  });
  // stub for function call
  // transId => Function
  this.stub = {};

  this.chunkStream = {};

  this.enterCount = 0;
  this.leaveCount = 0;
  this.clientId = generateClientID(this.server ? this.server.sessions : {});
}

inherits(NetConnection, Writable);

/**
 * Mixin `Parser`.
 */
Parser(NetConnection.prototype);

// default chunk-size 128 byte, max chunk size 65535 byte.
NetConnection.prototype.inChunkSize		= 128;
NetConnection.prototype.outChunkSize	= 128;
NetConnection.prototype.maxChunkSize	= 65535;

NetConnection.Role = {
	IN	: 0x01,
	OUT : 0x02,
};

NetConnection.STATE = {
	INIT		: 0x01,
	IN_SERVER	: 0x02
};

NetConnection.prototype.onCycle = function onCycle()
{

};
/**
 * @brief: send ping packet to peer.
 * @checked
 */
NetConnection.prototype.ping = function(){
	 debug('Function[ping] UserControlMessages: PingRequest');
	 var buffer = this.encoder.UserControlMessageEvents(0x06, Math.ceil(Date.now()/1000));
	 this.socket.write(buffer);
};

/**
 * @brief: get statistics info
 * @ bytesRead		: {Number} 
 * @ bytesWritten	: {Number}
 */
NetConnection.prototype.stat = function (){
	if(this.socket){
		return {bytesRead: this.socket.bytesRead, bytesWritten: this.socket.bytesWritten};	
	}	
	return null;
};

/**
 * @brief: connect to remote rtmp server.
 * @param: opts.vhost {String} VirtualHost.
 * @param: opts.host  {String} remote host.
 * @param: opts.port  {Number} remote port.
 * @param: opts.app	  {String} remote app.
 */
NetConnection.prototype.connect = function connect(opts, cb){
    console.log("Function[NetConnection.connect], opts", JSON.stringify(opts));
    this.vhost  = opts.vhost || '';
    this.host   = opts.host || '127.0.0.1';
    this.port   = opts.port || 1935;
    this.app    = opts.app  || 'live';
    var transId = this.nextTransId++;
    var self    = this;
	console.log("NetConnection transId %d", transId);
    this.stub[transId] = function(err, event){
        console.log("Function[NetConnection.connect]'s callback] enter", err, JSON.stringify(event));
		self.emit("connect");
        cb(err, event);
        console.log("Function[NetConnection.connect]'s callback] leave", err, JSON.stringify(event));
    };

    this.socket = net.connect(opts, function(err){
         if(err){
             delete self.sub[transId];
             return cb(err);
         } 
         //send c0 + c1
         self.c0 = new Buffer([0x03]);
         self.c1 = new Buffer(1536);
         self.c1.fill(0x0);
         self.socket.write(self.c0);
         self.socket.write(self.c1);
    });
    this.socket.on('error', function(err){
		console.log("Function[NetConnection.connect] enter, socket.onError, err", err);
        cb(err);        
        self.emit('error', err);
		console.log("Function[NetConnection.connect] leave, socket.onError, err", err);
    });
    this.socket.on('close', function(err){
		console.log("Function[NetConnection.connect] close, enter,  err", err);
        self.emit('close', err);
		console.log("Function[NetConnection.connect] close, leave,  err", err);
    });
    this.socket.pipe(this);

    this.on('s2',function(buf){
        // (1) send connect('connect');
        (function(){
            var objs = [
            {type: amf0Types.kStringType, value: "connect"},
            {type: amf0Types.kNumberType, value: transId},
            {type: amf0Types.kObjectType, value: 
                [
                {type:amf0Types.kStringType,  key:"app",  value: opts.app}, 
                //{type:amf0Types.kStringType,  key:"flashVer", value: "LNX 9,0,124,2"}, 
                {type:amf0Types.kStringType,  key:"tcUrl", value: "rtmp://" + opts.host + ":" + opts.port + "/" + opts.app}, 
                {type:amf0Types.kStringType,  key:"type",  value: "nonprivate"},
                {type:amf0Types.kStringType,  key:"flashVer", value: "FMLE/3.0 (compatible; FMSc/1.0"}, 
                {type:amf0Types.kStringType,  key:"swfUrl", value: "rtmp://" + opts.host + ":" + opts.port + "/" + opts.app}, 
                {type:amf0Types.kBooleanType, key:"fpad", value: false}, 
                {type:amf0Types.kNumberType,  key:"capabilities", value: 15}, 
                {type:amf0Types.kNumberType,  key:"audioCodecs", value: 4071}, 
                {type:amf0Types.kNumberType,  key:"videoCodecs", value: 252}, 
                {type:amf0Types.kNumberType,  key:"videoFunction", value:1}, 
                ]
            }
           ];

           var chunks = [];
           objs.forEach(function(val, idx, arr){
                   debug(val);
                   chunks.push(amf.write(val));        
           });
           var msg = Buffer.concat(chunks);
           var info = { fmt: 0, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
           var buffer = self.encoder.AMF0CmdMessage(info);
           self.socket.write(buffer);
        }.call(this));

        //send windowacksize
        (function(){
            var buffer = self.encoder.WindowAcknowledgementSize(250*10000);
            self.socket.write(buffer);
         }.call(this));
    });
};

/**
 * @brief: create stream when NetConnection connect remote rtmpServer as client.
 * @param: cb {Function}
 */
NetConnection.prototype.createStream = function(cb, streamName){
    console.log('Function[NetConnection.createStream] enter');
    var self = this;
    var transId = this.nextTransId++;
    this.stub[transId] = function(err, streamId){
        debug('Function[NetConnection.prototype.createStream callback]', err);
        console.log('Function[NetConnection.prototype.createStream callback]', err);
        var opts = {streamId: streamId, vhost: self.vhost, app: self.app, netConn: self };
        var ns = new NetStream(opts);
        self.setNetStreamById(streamId, ns);
        cb(err, ns); 
    };

    //send createStream
    (function(){
     var objs = [
     {type: amf0Types.kStringType, value: "createStream"},
     {type: amf0Types.kNumberType, value: transId},
     {type: amf0Types.kNullType,   value: null},
     ];
     var chunks = [];
     objs.forEach(function(val, idx, arr){
         chunks.push(amf.write(val));        
         });
     var msg = Buffer.concat(chunks);
     var info = { fmt: 1, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
     var buffer = this.encoder.AMF0CmdMessage(info);
     this.socket.write(buffer);
     }.call(this));

    //send _checkbw 
    transId = this.nextTransId++;
    (function(){
         var objs = [
         {type: amf0Types.kStringType, value: "_checkbw"},
         {type: amf0Types.kNumberType, value: transId},
         {type: amf0Types.kNullType,   value: null},
         ];
         var chunks = [];
         objs.forEach(function(val, idx, arr){
             chunks.push(amf.write(val));        
             });
         var msg = Buffer.concat(chunks);
         var info = { fmt: 1, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
         var buffer = this.encoder.AMF0CmdMessage(info);
         this.socket.write(buffer);
     }.call(this));

    console.log('Function[NetConnection.createStream] leave');
};

/**
 * @brief: create stream when NetConnection connect remote rtmpServer as client.
 * @param: cb {Function}
 */
NetConnection.prototype.fmleCreateStream = function(streamName, cb){
    console.log('Function[NetConnection.createStream] enter');
    var self = this;

	// (1) send releaseStream
    console.log('Function[NetConnection.createStream] releaseStream enter');
    var transId = this.nextTransId++;
    (function(){
         var objs = [
         {type:amf0Types.kStringType,	value: "releaseStream"},
         {type:amf0Types.kNumberType,	value: transId},
         {type:amf0Types.kNullType,		value: null},
         {type:amf0Types.kStringType,	value: streamName},
         ];
         var chunks = [];
         objs.forEach(function(val, idx, arr){
             chunks.push(amf.write(val));        
             });
         var msg = Buffer.concat(chunks);
         var info = { fmt: 1, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
         var buffer = this.encoder.AMF0CmdMessage(info);
         this.socket.write(buffer);
     }).call(this);
    console.log('Function[NetConnection.createStream] releaseStream leave');

	// (2) send FCPublish
    console.log('Function[NetConnection.createStream] FCPublish enter');
    transId = this.nextTransId++;
    (function(){
         var objs = [
         {type:amf0Types.kStringType,	value: "FCPublish"},
         {type:amf0Types.kNumberType,	value: transId},
         {type:amf0Types.kNullType,		value: null},
         {type:amf0Types.kStringType,	value: streamName},
         ];
         var chunks = [];
         objs.forEach(function(val, idx, arr){
             chunks.push(amf.write(val));        
             });
         var msg = Buffer.concat(chunks);
         var info = {fmt: 1, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
         var buffer = this.encoder.AMF0CmdMessage(info);
         this.socket.write(buffer);
     }).call(this);
    console.log('Function[NetConnection.createStream] FCPublish leave');

    //send createStream
    transId = this.nextTransId++;
    this.stub[transId] = function(err, streamId){
        debug('Function[NetConnection.prototype.createStream callback]', err);
        console.log('Function[NetConnection.prototype.createStream callback]', err);
        var opts = {streamId: streamId, vhost: self.vhost, app: self.app, netConn: self };
        var ns = new NetStream(opts);
        self.setNetStreamById(streamId, ns);
        cb(err, ns); 
    };
    (function(){
     var objs = [
     {type: amf0Types.kStringType, value: "createStream"},
     {type: amf0Types.kNumberType, value: transId},
     {type: amf0Types.kNullType,   value: null},
     ];
     var chunks = [];
     objs.forEach(function(val, idx, arr){
         chunks.push(amf.write(val));        
         });
     var msg = Buffer.concat(chunks);
     var info = {fmt: 1, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
     var buffer = this.encoder.AMF0CmdMessage(info);
     this.socket.write(buffer);
     }.call(this));

    //send _checkbw 
    transId = this.nextTransId++;
    (function(){
         var objs = [
         {type: amf0Types.kStringType, value: "_checkbw"},
         {type: amf0Types.kNumberType, value: transId},
         {type: amf0Types.kNullType,   value: null},
         ];
         var chunks = [];
         objs.forEach(function(val, idx, arr){
             chunks.push(amf.write(val));        
             });
         var msg = Buffer.concat(chunks);
         var info = { fmt: 1, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
         var buffer = this.encoder.AMF0CmdMessage(info);
         this.socket.write(buffer);
     }.call(this));

    console.log('Function[NetConnection.createStream] leave');
};

/**
 * active close the netconnect
 *
 */
NetConnection.prototype.close = function (){
    console.log("Function[NetConnection.close] enter");
    NetConnection.prototype.onClose.call(this, null);
    this.socket && this.socket.close();
    console.log("Function[NetConnection.close] leave");
};

/**
 * @brief: create NetStream Instance
 * @param: streamId {Number} stream id.
 */
NetConnection.prototype.createNetStream = function(streamId){
    debug('Function[NetConnection.proto.createNetStream]');
    var streamId = streamId || this.nextStreamId++;
    var self = this;
    var opts = {streamId: streamId,vhost:this.vhost, app: this.app, netConn: self };
    var ns =  new NetStream(opts);
    this.setNetStreamById(streamId, ns);
    return ns;
}

/*---------------------------------------------------------*/
/**
 * @brief: NetStream Object.
 *
 * @param:  netConn		{NetConnection}
 * @param:	streamId	{Number}
 * @param:	vhost		{String}
 * @param:	app			{String}
 *
 * @member:	streamName	{String}
 * @member:	metadata	{Object} MetaData 
 * @member:	aacSeqHdr	{Object} AAC Sequence Header.
 * @member:	avcSeqHdr	{Object} AVC Sequence Header.
 *
 * @member:	msgQueue	{Array} 
 * @member:	role		{Enum}
 * @member:	startTime	{Date}
 * @member:	currentTime	{Date}
 * @member:	subscribers	{Array}
 * @member:	prevAudioTime	{Number}
 * @member: prevVideoTime:	{Number}
 * @member: timerId:		{Timer}
 * @member: queueStartTime:	{Number}
 * @member: queueEndTime:	{Number}
 * @member: isPlaying:		{Boolean}
 * @member: waitForKeyFrame	{Boolean}
 * @member: publisher		{Object}
 * @member: paused			{Object}
 * @member: pauseDuration	{Object}
 * @member: flags			{Number}
 */

function NetStream(opts){
    if(!(this instanceof NetStream)){
        return new NetStream(opts); 
    }
    this.netConn  = opts.netConn;
    this.streamId = opts.streamId || 0;
    this.vhost = opts.vhost	|| '';
    this.app   = opts.app || '';
    this.streamName = '';

    this.metadata   = null;
    this.aacSeqHdr  = null;
    this.avcSeqHdr  = null;

    //GOP: Group Of Picture
    this.msgQueue   = [];
    this.role       = NetStream.prototype.Role.Idle;
    this.startTime  = 0;
    this.currentTime    = 0;
    this.subscribers    = [];

    this.prevAudioTime = 0;
    this.prevVideoTime = 0;

    this.timerId = null;

    this.queueStartTime = 0;
    this.queueEndTime   = 0;

    //this.flv = fs.openSync('./immt.flv', 'w');

    this.isPlaying = false;
    this.waitForKeyFrame = false;
    this.audioCount = 0;
    this.videoCount = 0;

    this.publisher = null;

	this.on('publish',      NetStream.prototype.onPublish.bind(this));
	this.on('FCUnpublish',  NetStream.prototype.onUnpublish.bind(this));
	this.on('onStatus',     NetStream.prototype.onStatus.bind(this));
    this.on('closeStream',  NetStream.prototype.onClose.bind(this));
    this.on('close',        NetStream.prototype.onClose.bind(this));

    this.paused = false;
    this.pauseDuration = 0;

	this.flags = 0;
	this.state = NetStream.STATE.INIT;

	this.closed = false;
}

inherits(NetStream, require('events').EventEmitter);

NetStream.prototype.Role = {
    Idle    : 0,
    Publish : 1,
    Play    : 2,
	Edge	: 4,
};

NetStream.prototype.Flags = {
	BUBBLE			: 0x01,
	ACTIVE_CHECK	: 0x02,
	PIPE			: 0x04,
};

NetStream.STATE = {
	INIT			: 0x01,
	PUBLISH			: 0x02,
	STOP_PUBLISH	: 0x04,
	PLAY			: 0x08,
	STOP_PLAY		: 0x10,
	CLOSE			: 0x20,
};

NetStream.prototype.startCycle = function (){
    console.log("Function[NetStream.startCycle] enter");
    this.netConn && this.netConn.server && this.netConn.server.thread.pushTask(this);
    console.log("Function[NetStream.startCycle] leave");
};

NetStream.prototype.stopCycle = function (){
    console.log("Function[NetStream.stopCycle] enter");
    this.netConn && this.netConn.server && this.netConn.server.thread.removeTask(this);
    console.log("Function[NetStream.stopCycle] leave");
}

NetStream.prototype.onCycle = function (interval){
    debug("Function[NetStream.onCycle enter");
    var count = 0;
    debug("Function[NetStream.onCycle Msg.length ", this.msgQueue.length);
    while(this.msgQueue.length > 0 && count++ < 10){
        var msg = this.msgQueue.shift();

        if (this.state == NetStream.STATE.STOP_PLAY){
            continue; 
        }

        if(this.waitForKeyFrame === true){
            if(msg.type !== 0x09){
                return; 
            }
            if(!msg.isKeyFrame){
                return; 
            }
            this.waitForKeyFrame = false; 
        }
        if(msg.type === 0x08){
            this.sendAudio(msg); 
        }else if(msg.type === 0x09){
            this.sendVideo(msg);
        }else {
        }
    }
    debug("Function[NetStream.onCycle leave");
};

NetStream.prototype.setRolePublish = function setRolePublish(){
    debug("Function[%s] setRolePublish", arguments.callee.name);
	this.setRole(this.Role.Publish);
};

NetStream.prototype.setRolePlay = function setRolePlay(){
    debug("Function[%s] setRolePlay", arguments.callee.name);
	this.setRole(this.Role.Play);
};

NetStream.prototype.setRole = function setRole(role){
	this.role |= role;
};

NetStream.prototype.clearRole= function(){
    debug("Function[%s] clearRole", arguments.callee.name);
    this.role = this.Role.Idle;
};

NetStream.prototype.onPublish = function (args){
	console.log("Function[NetStream.onPublish] enter state: %d", this.state);
    var streamName = args[3];
    this.setName(streamName);
    this.setRolePublish();
    this.setFlags(this.Flags.PIPE);
	this.state = NetStream.STATE.PUBLISH;
    var pool = this.netConn.server.publishPool;
	if (pool) {
		var req = {vhost: this.vhost, app: this.app, streamName: this.streamName};
        PublishPool.prototype.onPublish.call(pool, req, this);
	}
	console.log("Function[NetStream.onPublish] leave state: %d, emit publish: ", this.state, !!pool);
};
NetStream.prototype.onUnpublish = function (){
	console.log("Function[NetStream.onUnpublish] state: %d", this.state);
	assert(this.state === NetStream.STATE.PUBLISH);
	this.state = NetStream.STATE.STOP_PUBLISH;
    var pool = this.netConn.server.publishPool;
	if (pool) {
		var req = { vhost: this.vhost, app: this.app, streamName: this.streamName};
		//pool.emit("unpublish", req, this);
		console.log("Function[NetStream.onUnpublish] state: %d, req: %s", this.state, JSON.stringify(req));
		PublishPool.prototype.onUnpublish.call(pool, req, this);
	} else {
		console.log("Function[NetStream.onUnpublish] leave state: %d, not found publishPool", this.state);
	}
};


NetStream.prototype.onStatus = function (args){
    var pool = this.netConn.server.publishPool;
	if (pool) {
		var req = {vhost: this.vhost, app: this.app, streamName: this.streamName};
		pool.emit("onStatus", req, this, args);
	}
};

/**
 * @param: args[0] {String} Command Name
 * @param: args[1] {Number} Transaction ID
 * @param: args[2] {null}
 * @param: args[3] {String} streamName
 * @param: args[4] {Number} -1, 
 */
NetStream.prototype.onPlay = function(args){
    console.log("Function[NetStream.onPlay] enter");
    var streamName = args[3];

    this.pause(false, 0);
    this.setName(streamName);
    this.setRolePlay();

    var pool = this.netConn.server.publishPool;
    var req = {
        vhost: this.vhost,
        app:   this.app,
        streamName: this.streamName
    };
    console.log('publishPool req, get', req);
    var pipe = pool.get(req);
    if (!pipe){
        console.log("Not Found Pipe", JSON.stringify(req));
        pipe = new Pipe(req);
        pool.set(req, pipe);
        pipe.edgePlay = new EdgePlay({server: this.server, pipe: pipe, vhost: ns.vhost, app: ns.app, streamName: ns.streamName});
        pipe.edgePlay.connect({
            host: 'cp01-wise-2011q4ecom05.cp01.baidu.com',
            port: 8935,
        });
    }
    console.log("Found Pipe");

    this.acceptPlay();
    pipe.emit("play", this);
    this.emit('play', args);
    console.log("Function[NetStream.onPlay] enter");
};

NetStream.prototype.onPlay2 = function (args){
    this.setRolePlay();
    this.emit(args[0], args);
};

NetStream.prototype.onDeleteStream = function(args){
    this.clearRole();
    this.netConn.emit('deleteStream', this);
    this.onClose();
};

/**
 * @brief: clear when on close.
 * @role is play: remove self from publish's subscriber list.
 * @role is publish: stop publish, remove subscriber.
 */
NetStream.prototype.onClose = function onClose(err){
	console.log("Function[NetStream.onClose] this.closed: %d, this.state: %d", this.closed, this.state, err);
	if (this.closed){
		console.log("NetStream.onClose repeat close, this.closed: %d", this.closed);
		return; 
	}
	this.closed = true;

    this.stopCycle();

	switch(this.state){
		case NetStream.STATE.PUBLISH:
			var pool = this.netConn.server.publishPool;
			var req  = {vhost: this.vhost, app: this.app, streamName: this.streamName};
			pool.emit("unpublish", req, this,  (this.role & this.Role.Publish) ? true: false)
			break;
		case NetStream.STATE.PLAY:
			/**
			  var pool = this.netConn.server.publishPool;
			  var req = {vhost: this.vhost, app: this.app, streamName: this.streamName};
			  pool.emit("FCUnpublish", req, this,  (this.role & this.Role.Publish) ? true: false)
			 **/
			break;	
		default:
			break;
	}
	console.log("Function[NetStream.onClose] called", this.closed, this.role);
	// pool 

	if(this.publisher){
		this.publisher.removeSubscriber(this); 
	} 

	if((this.role & this.Role.Publish)){
		console.log("Function[NetStream.onClose] Publisher called", this.closed, this.role);
		debug('Function[%s] NetStream onClose', arguments.callee.name);
		//this.netConn.server.publishPool.del({ vhost: this.vhost, app  : this.app, streamName: this.streamName }); 

		var pool = this.netConn.server.publishPool;
		var req = {vhost: this.vhost, app: this.app, streamName: this.streamName};
		pool.emit("closeStream", req, this, true);
		this.role = this.Role.Idle;
	}
	if ((this.role & this.Role.Play)){
		console.log("Function[NetStream.onClose] Player called", this.closed, this.role);
		var pool = this.netConn.server.publishPool;
		var req = {vhost: this.vhost, app: this.app, streamName: this.streamName};
		pool.emit("closeStream", req, this, false);
		this.role = this.Role.Idle;
	}
	// deal with all subscribers;
	for(var i=0, length = this.subscribers; i < length; i++){
		var ns = this.subscribers[i];  
		ns.emit('closeStream');
	}
	// deal with timerId
	if(this.timerId){
		clearInterval(this.timerId);
		this.timerId = null;
	}
	// clear MessageQueue
	this.msgQueue = [];

};

/**
 * @brief: client send 'pause' command
 * @toggle:		{Boolean} 
 * @millSeconds	{Number}
 */
NetStream.prototype.pause = function(toggle, millSeconds){
    this.paused         = toggle; 
    this.pauseDuration  = millSeconds;
};

NetStream.prototype.publish = function (streamName, cb){
    console.log("Function[NetStream.publish] streamName: %s", streamName);

    this.streamName = streamName;
    this.publishCallback = cb;

    var nc = this.netConn;
    var ns = this;

    //(0) SetChunkSize
    nc.socket.write(nc.encoder.setChunkSize(CONSTS.outChunkSize));

    var transId = this.netConn.nextTransId++;
    (function(){
         var objs = [
         {type:amf0Types.kStringType,	value: "publish"},
         {type:amf0Types.kNumberType,	value: transId},
         {type:amf0Types.kNullType,		value: null},
         {type:amf0Types.kStringType,	value: streamName},
         {type:amf0Types.kStringType,	value: "live"},
         ];
         var chunks = [];
         objs.forEach(function(val, idx, arr){
             chunks.push(amf.write(val));        
             });
         var msg = Buffer.concat(chunks);
         var info = {fmt:0, csid: 8, msg:msg, msgLength:msg.length, streamId:ns.streamId};
         var buffer = nc.encoder.AMF0CmdMessage(info);
         nc.socket.write(buffer);
     }.call(this));
};

NetStream.prototype.stopPublish = function (cb){
    console.log("Function[NetStream.stopPublish] enter, state: %d", this.state);
	var self = this;
    this.state = NetStream.STATE.STOP_PLAY;
    var transId = this.netConn.nextTransId++;
	this.netConn.stub[transId] = function (){
        console.log("FCUnpublish response");
		self.emit("unpublish");
		self.netConn.stub[transId] = null;
        cb && cb(); 
	};
	// (1) send FCUnpublish
    (function(){
         var objs = [
         {type:amf0Types.kStringType,	value: "FCUnpublish"},
         {type:amf0Types.kNumberType,	value: transId},
         {type:amf0Types.kNullType,		value: null},
         {type:amf0Types.kStringType,	value: this.streamName},
         ];
         var chunks = [];
		 objs.forEach(function(val, idx, arr){
			 chunks.push(amf.write(val));        
		 });
         var msg = Buffer.concat(chunks);
         var info = {fmt:1, csid: 3, msg: msg, msgLength: msg.length, streamId: self.streamId};
         var buffer = this.netConn.encoder.AMF0CmdMessage(info);
         this.netConn.socket.write(buffer);
     }.call(this));

    // deleteStream
    transId = this.netConn.nextTransId++;
    (function(){
         var objs = [
         {type:amf0Types.kStringType,	value: "deleteStream"},
         {type:amf0Types.kNumberType,	value: transId},
         {type:amf0Types.kNullType,		value: null},
         {type:amf0Types.kNumberType,	value: this.streamId},
         ];
         var chunks = [];
		 objs.forEach(function(val, idx, arr){
			 chunks.push(amf.write(val));        
		 });
         var msg = Buffer.concat(chunks);
         var info = {fmt:1, csid: 3, msg: msg, msgLength: msg.length};
         var buffer = this.netConn.encoder.AMF0CmdMessage(info);
         this.netConn.socket.write(buffer);
     }.call(this));
    console.log("Function[NetStream.stopPublish] leave, state: %d", this.state);
};

NetStream.prototype.play = function(streamName, cb){
    console.log("Function[NetStream.play] enter, streamName: ", streamName);

    var transId = this.netConn.nextTransId++;
    this.playCallback = cb;
    this.setName(streamName); 

    var nc = this.netConn;
    var ns = this;

    // send Play Command
    (function(){
         var objs = [
         {type:amf0Types.kStringType, value: "play"},
         {type:amf0Types.kNumberType, value: transId},
         {type:amf0Types.kNullType,   value: undefined},
         {type:amf0Types.kStringType, value: streamName},
         {type:amf0Types.kNumberType, value: -2},
         ];
         var chunks = [];
         objs.forEach(function(val){
             chunks.push(amf.write(val));        
         });
         var msg = Buffer.concat(chunks);
         var info = {fmt:0, csid:8, msg:msg  , msgLength:msg.length, streamId:ns.streamId};
         var buffer = nc.encoder.AMF0CmdMessage(info);
         nc.socket.write(buffer);
     }.call(this));

    (function(){
         var buffer = nc.encoder.UserControlMessageEvents(0x03, ns.streamId, 3000);
         debug('UserControlMessages: Set Buffer Length ');
         nc.socket.write(buffer);
     }.call(this));

    console.log("Function[NetStream.play] leave, streamName: %s", streamName);
};

/**
 * @brief : Edge Server pipe data to Origin  pipe data to 
 */
/**
NetStream.prototype.pipe = function (dst_){
	this.appendSubscriber(dst_);
};
**/

/**
 * @brief: put self into publish pool.
 */
/**
NetStream.prototype.edge = function(req){
    req.netStream = this;
    this.netConn.server.publishPool.set(req, new Pipe({publisher: req.netStream, vhost: req.netStream.}) );

    var publisher = this.netConn.server.publishPool.get(req);
    if(publisher){
        ns.setPublisher(publisher);
        ns.netConn.acceptPlay(ns.streamId);
        publisher.appendSubscriber(ns);
        return true;
    }
    return false;
};
**/

NetStream.prototype.appendSubscriber = function(ns){
    debug("Function[%s] play %s","appendSubscriber", ns.streamName);
    this.subscribers.push(ns);
    if(this.metadata){
        debug("Function[%s] %s write metadata","appendSubscriber", ns.streamName, this.metadata);
        ns.sendMetadata(this.metadata); 
    }
    if(this.aacSeqHdr){
        debug("Function[%s] %s write aacSeqHdr","appendSubscriber", ns.streamName, this.aacSeqHdr);
        ns.sendAudio(this.aacSeqHdr); 
    } 
    if(this.avcSeqHdr){
        debug("Function[%s] %s write avcSeqHdr","appendSubscriber", ns.streamName, this.avcSeqHdr);
        ns.sendVideo(this.avcSeqHdr);
    }

    
    for(var i = 0; i < this.msgQueue.length; i++){
        if(i === 0){
            debug('Function[appendSubscriber] GOP');
        }
        ns.msgQueue.push(this.msgQueue[i]); 
    }

    ns.waitForKeyFrame = true;

    var self = ns;

    ns.timerId = setInterval(function(){
      var count = 0;
      while(self.msgQueue.length > 0 && count++ < 5){
            var msg = self.msgQueue.shift();

            if(ns.waitForKeyFrame === true){
               if(msg.type !== 0x09){
                    return; 
               }
               if(!msg.isKeyFrame){
                    return; 
               }
               ns.waitForKeyFrame = false; 
            }
            if(msg.type === 0x08){
                //debug("Function[%s] Type[%d] isKeyFrame[%d]","NetStream.Timeout", msg.type, msg.isKeyFrame);
                self.sendAudio(msg); 
            }else if(msg.type === 0x09){
                //debug("Function[%s] Type[%d] isKeyFrame[%d]","NetStream.Timeout", msg.type, msg.isKeyFrame);
                self.sendVideo(msg);
            }else {
                //debug("Function[%s] unknow msg","NetStream.Timeout", msg);
            }
      }
    },
    50);
};

NetStream.prototype.removeSubscriber = function(ns){
    for(var i = 0, length = this.subscribers.length; i < length; i++){
        if(ns === this.subscribers[i]){
            this.subscribers.splice(i,1); 
            break;
        }
    }
}

NetStream.prototype.setPublisher = function(ns){
    this.publisher = ns;
};

/**
 * @brief: write Metadata to player/relay whatever.
 * @param: Msg: Message
 **/
NetStream.prototype.sendMetadata = function(Msg){
    var opts = {
        fmt     : 0,
        csid    : 4, 
        timestamp:  0,
        msgLength:  Msg.length,
        msgType :   0x12,
        streamId:   this.streamId,
        rawData :   Msg.rawData
    };
    debug('Function[%s] called', 'sendMetadata');
    var c = this.netConn.encoder.MetadataMessage(opts);
    if(c){
        this.netConn.send(c);
    }
};

/**
 * @brief: write Audio to player/relay whatever.
 * @param: Msg: Message
 **/
NetStream.prototype.sendAudio = function sendAudio(Msg){
    var opts = {
        fmt     : Msg.fmt,
        csid    : 4, 
        timestamp:  Msg.timestamp,
		absTimestamp: Msg.absTimestamp,
        msgLength:  Msg.length,
        msgType :   Msg.type,
        streamId:   this.streamId,
        rawData :   Msg.rawData
    };
	//debug("function[sendAudio] ", opts);
    var header = this.netConn.encoder.makeRTMPHeader(opts);
    if(header){
		//debug("function[sendAudio] send header", header);
        this.netConn.send(header); 
        this.netConn.send(Msg.body);
    }
    /**
    var c = this.netConn.encoder.AudioMessage(opts);
    if(c){
        this.netConn.send(c);
    }
    **/
};

/**
 * @brief: write Video to player/relay whatever.
 * @param: Msg: Message
 **/
NetStream.prototype.sendVideo = function sendVideo(Msg){

    var opts = {
        fmt     : Msg.fmt,
        csid    : 6, 
        timestamp:  Msg.timestamp,
		absTimestamp: Msg.absTimestamp,
        msgLength:  Msg.length,
        msgType :   Msg.type,
        streamId:   this.streamId,
        rawData :   Msg.rawData
    };

	//debug("function[sendVideo] ", opts);
    var header = this.netConn.encoder.makeRTMPHeader(opts);
    if(header){
		//debug("function[sendVideo] send header", header);
        this.netConn.send(header); 
        this.netConn.send(Msg.body);
    }
    return;

    /**
    var c = this.netConn.encoder.VideoMessage(opts);
    if(c){
        this.netConn.send(c);
    }
    **/
};

NetStream.prototype.isAacSeqHdr = function(buff){
    if(buff && buff.length >= 2 && (buff[0]>>4) == 0x0a && buff[1] == 0x00){ 
        return true;
    }
    return false;
};

NetStream.prototype.recvMetadata = function(msg){
    debug("Function[%s] recvMetadata", "recvMetadata", msg.length);
    this.metadata = msg;
    this.msgQueue.push(msg);

    debug("Function[recvMetadata] copy message to all subscriber: %d",ns.subscribers.length);
	if ((this.flags & this.Flags.PIPE)) {
        console.log("Function[NetStream.recvMetadata]");
		this.emit('metadata', msg);
	}
    if (this.pipe){
        this.pipe.onMetaData(msg);
    }

	/**
	for(var i=0; i < ns.subscribers.length; i++){
		var sub = ns.subscribers[i];  
		sub.recvMetadata(msg);
	}
	**/

    /*
    var flvHeader = new Buffer([0x46,0x4c,0x56,0x01,0x05,0x00,0x00,0x00,0x09, 0x00,0x00,0x00,0x00]);
    fs.writeSync(this.flv, flvHeader, 0, flvHeader.length);

    var content = MakeTag({tagType: 0x12, dataSize: msg.rawData.length, timestamp: msg.timestamp, data: msg.rawData});

    fs.writeSync(this.flv, content, 0, content.length, null)
    */
};

/**
 *{
 * type: 9,
 * size: 10019,
 * timestamp: 40,
 * absTime: 1403686478991506,
 * duration: 1640,
 * channelId: 4,
 * streamId: 1,
 * data:
 * [ <SlowBuffer 27 01 00 01 b8 00 00 27 1a 41 9a 33 82 3b 69 72 a7 4b 32 74 b3 27 44 a1 d8 4a 52 ff 75 b6 01 1a 1c b0 8e 22 01 cb b9 f3 f3 fe b6 27 7c 53 a2 65 e3 d3 5c ...>,
 *   <SlowBuffer 51 78 b1 2b 79 bd 8c 90 29 0e 01 74 c9 e3 16 db b1 7f b9 ef 24 f5 89 0b 1b 18 bf b3 78 fc 4c f7 cb 93 57 5a d7 b0 7e 5f dd 0a 05 ff 7e 06 13 b0 43 1a e5 ...> ] }
 */

NetStream.prototype.recvAudio = function(msg){
    if (this.state == NetStream.STATE.STOP_PLAY) {
        return; 
    }
    if(msg.fmt === 0){
		/**
        var delta = msg.absTimestamp - this.prevAudioTime; 
        if(delta > 0){
           msg.timestamp = delta;  
        }else {
           msg.timestamp = 46; 
        }
		**/
        this.prevAudioTime = msg.absTimestamp; 
    }else { // msg.timestamp is timestampDelta
        this.prevAudioTime += msg.timestamp; 
    }

    msg.duration = msg.absTimestamp = this.prevAudioTime;
    debug('Function[recvAudio] fmt: %d, timestamp: %d, absTimestamp: %d',msg.fmt, msg.timestamp, msg.absTimestamp);

    if(this.isAacSeqHdr(msg.rawData)){
        debug("Function[%s] stream:%s recv aacSeqHdr", "recvAudio", this.streamName, msg);
        this.emit("aacSequenceHeader", msg);
        msg.csid = 9;
        this.aacSeqHdr = msg; 
        this.aacConfig = AudioSpecificConfig(msg.rawData.slice(2));
        debug("AudioSpecificConfig", this.aacConfig);
        this.msgQueue.push(msg);
    }else {
        if(this.paused){
            return;
        }
        this.msgQueue.push(msg);
    }

	if((this.flags & this.Flags.BUBBLE)){
		this.emit('data', msg);	
	}

	if ((this.flags & this.Flags.PIPE)) {
		if(this.isAacSeqHdr(msg.rawData)){
            console.log("Function[NetStream.recvAudio] AacSeqHdr");
			this.emit('aacSeqHdr', msg);
		} else {
			this.emit('audio', msg);
		}
	}

    if (this.pipe){
		if(this.isAacSeqHdr(msg.rawData)){
			this.pipe.onAacSeqHdr(msg);
		} else {
            this.pipe.onAudio(msg);
		}
    }

    this.audioCount++;
    if (this.audioCount > 100){ // pure Audio stream
        this.waitForKeyFrame = false; 
    }


    /**
    var content = MakeTag({tagType: 0x08, dataSize: msg.rawData.length, timestamp: this.prevAudioTime, data: msg.rawData});

    fs.writeSync(this.flv, content, 0, content.length, null)
    **/
};

function isAvcSeqHdr(buff){
    if(buff && buff.length >= 2 && buff[0]== 0x17 && buff[1] == 0x00){ 
        return true;
    }
    return false;
};

NetStream.prototype.isAvcSeqHdr = isAvcSeqHdr;

NetStream.prototype.recvVideo = function(msg){

    if (this.state == NetStream.STATE.STOP_PLAY) {
        return; 
    }

    if(msg.fmt === 0){ //absolute timestamp
		/**
        var delta = msg.absTimestamp - this.prevVideoTime;;
        if(delta > 0){
            msg.timestamp = delta; 
        }else {
            msg.timestamp = 40; 
        }
		**/
        this.prevVideoTime = msg.absTimestamp; 
    }else {
        this.prevVideoTime += msg.timestamp; 
    }

    msg.duration = msg.absTimestamp = this.prevVideoTime;
    debug('Function[recvVideo] fmt: %d, timestamp: %d, absTimestamp: %d',msg.fmt, msg.timestamp, msg.absTimestamp);

    /*******
    if(this.queueStartTime === 0){
        debug('Function[recvVideo] queueStartTime: %d, prevVideoTime:%d', this.queueStartTime, this.prevVideoTime);
        this.queueStartTime = this.prevVideoTime;
    }
    **/
    this.queueEndTime = this.prevVideoTime;

    if(this.isAvcSeqHdr(msg.rawData)){
        debug("Function[%s] stream:%s recv avcSeqHdr", "recvVideo", this.streamName, msg);
        this.emit("avcSequenceHeader", msg);
        msg.csid = 10;
        this.avcSeqHdr = msg; 
        this.avcConfig = AVCDecoderConfigurationRecord(msg.rawData.slice(5));
        debug("AVCDecoderConfigrautiona", this.avcConfig);
        this.msgQueue.push(msg);
    }else{
        if(this.paused){
            return; 
        }
        this.msgQueue.push(msg);

        if(msg.isKeyFrame && (this.queueEndTime - this.queueStartTime > 10000)){
           for(var i = 0; i < this.msgQueue.length; i++){
                if(this.msgQueue[i].type === 0x09 &&
                   this.msgQueue[i].isKeyFrame && 
                   (this.queueEndTime - this.msgQueue[i].absTimestamp <= 10000) &&
                   (this.queueEndTime - this.msgQueue[i].absTimestamp >=  5000) ){

                   debug('shrink [before] msg.isKeyFrame: %s, queueStartTime: %d, queueEndTime: %d, absTimestamp: %d', msg.isKeyFrame, this.queueStartTime, this.queueEndTime, this.msgQueue[0].absTimestamp);

                   this.queueStartTime = this.msgQueue[i].absTimestamp;

                   debug('shrink [after ] msg.isKeyFrame: %s, queueStartTime: %d, queueEndTime: %d, absTimestamp: %d', msg.isKeyFrame, this.queueStartTime, this.queueEndTime, this.msgQueue[i].absTimestamp);
                   this.msgQueue.splice(0, i-1); 
                   break;
                }
           }
        }
    }

	if((this.flags & this.Flags.BUBBLE)){
		this.emit('data', msg);
	}

	if ((this.flags & this.Flags.PIPE)) {
		if(this.isAvcSeqHdr(msg.rawData)){
			this.emit('avcSeqHdr', msg);
		} else {
			this.emit('video', msg);
		}
	}

    if (this.pipe){
		if(this.isAvcSeqHdr(msg.rawData)){
            this.pipe.onAvcSeqHdr(msg);
		} else {
            this.pipe.onVideo(msg);
		}
    }

    /**
    var content = MakeTag({tagType: 0x09, dataSize: msg.rawData.length, timestamp: this.prevVideoTime, data: msg.rawData});

    fs.writeSync(this.flv, content, 0, content.length, null)
    **/
};

NetStream.prototype.setName = function(name){
    this.streamName = name;
};

NetStream.prototype.setFlags = function setFlags(flags){
	console.log("Function[NetStream.setFlags], flags: ", flags);
	this.flags |= flags;
};

NetStream.prototype.acceptPublish = function(args){
    // (1) send NetConnection.Status.Success
    this.netConn.acceptPublish(this.streamId);
};

NetStream.prototype.acceptPlay = function(){
    this.netConn.acceptPlay(this.streamId);
};


NetConnection.prototype.headerSize = [11,7,3,0];
NetConnection.prototype.defaultChunkSize = 128;

NetConnection.prototype.setSocket = function(s){
    this.socket = s;
};

NetConnection.prototype.setVhost = function(vhost){
    this.vhost = vhost;
};

NetConnection.prototype.setApp = function(app){
    this.app = app;
};

NetConnection.prototype.send = function send(c){
    this.sendQueue.push(c); 
    var self = this;
    if(this.canSend){
        this.canSend = this.socket.write(this.sendQueue.shift(), function(){
            self.canSend = true; 
            self.emit('sendone');
        });
    }
}

/**
 * @param args[0] {String} Command Name 'connect'
 * @param args[1] {Number} Transaction ID 1
 * @param args[2] {Object} Command Object
 * @param args[3] {Object} Optional User Arguments
 *
 * @brief: set appName 
 */
NetConnection.prototype.onConnect = function NetConnOnConnect(args){
    console.log("Function[NetConnection.onConnect] enter, clientId[%s]", this.clientId);
    var O   = args[2];
    var app = O.app.replace(/\/$/, '');
    this.setApp(app);
    console.log("Function[NetConnection.onConnect] leave, clientId[%s]", this.clientId);
};

/**
 * @param err {Object} error-hint
 * @brief: close NetStream.
 * @brief: close self;
 */
NetConnection.prototype.onClose= function(err){
	console.log("Function[NetConnection.onClose] enter, err", err ? JSON.stringify(err) : err);
    if (this.state & RTMP_STATE_CLOSED) {
        console.error("Function[NetConnection.onClose] repeat close, clientId[%s]", this.clientId); 
        return;
    }
    this.state |= RTMP_STATE_CLOSED;

    for(var sid in this.netStreams){
        var ns = this.netStreams[sid];
        ns.emit('closeStream');
    }
    this.netStreams = {};
	console.log("Function[NetConnection.onClose] leave");
};

NetConnection.prototype.onCreateStream = function (args){
    console.log('Function[NetConnection.createStream] enter, clientId[%s]', this.clientId);
    var ns = this.createNetStream();
    var transId  = args[1];
    var streamId = ns.streamId;
    // (1) response to client
    (function(){
        var objs = [
        {type: amf0Types.kStringType, value: "_result"},
        {type: amf0Types.kNumberType, value: transId },
        {type: amf0Types.kNullType,   value: null},
        {type: amf0Types.kNumberType, value: streamId},
        ];
        var chunks = [];
        objs.forEach(function(val, idx, arr){
            chunks.push(amf.write(val));
        });
        var msg = Buffer.concat(chunks);
        var info = {fmt: 1, csid: 3, msg: msg, msgLength: msg.length, streamId:0};
        buffer = this.encoder.AMF0CmdMessage(info);
        this.socket.write(buffer);
    }).call(this);

    // (2) emit createStream event
    this.emit('createStream', ns);
    console.log('Function[NetConnection.createStream] leave, clientId[%s]', this.clientId);
};

NetConnection.prototype.onCloseStream = function(streamId){
    var ns = this.findNetStreamById(streamId);
    if (ns) {
        ns.onClose(); 
        this.deleteNetStreamById(streamId);
    }
};

NetConnection.prototype.onError = function(err){
    // Error object
    debug("Function[onError] called");
    if(err){
        debug("Function[NetConnection.onError] error", err); 
    }
};

NetConnection.prototype.onReleaseStream = function (args){
    //Response to releaseStream
   (function(){
       var transId = args[1];
       var objs = [
        {type: amf0Types.kStringType, value: "_result"},
        {type: amf0Types.kNumberType, value: transId },
        {type: amf0Types.kNullType,   value: null},
        {type: amf0Types.kUndefinedType,   value: undefined},
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg  = Buffer.concat(chunks);
       var info = { fmt: 0, csid: 3, msg: msg, msgLength: msg.length, streamId: 0 };
       buffer = this.encoder.AMF0CmdMessage(info);
       this.socket.write(buffer);
   }).call(this);
}

NetConnection.prototype.onFCPublish = function (args) {
    //Response to FCPublish
   (function(){
       var transId = args[1];
       var objs = [
        {type: amf0Types.kStringType, value: "_result"},
        {type: amf0Types.kNumberType, value: transId},
        {type: amf0Types.kNullType,   value: null},
        {type: amf0Types.kUndefinedType, value: undefined},
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg = Buffer.concat(chunks);
       var info = { fmt: 0, csid: 3, msg: msg, msgLength: msg.length, streamId:0};
       buffer = this.encoder.AMF0CmdMessage(info);
       this.socket.write(buffer);
   }).call(this);
};

NetConnection.prototype.findNetStreamById = function(streamId){
    if(streamId in this.netStreams){
        return this.netStreams[streamId]; 
    }else {
        debug('Not Found streamId:', streamId, this.netStreams);
		return null;
    }
};

NetConnection.prototype.findNetStreamByName = function(streamName){
   for(var streamId in this.netStreams){
		if (this.netStreams[streamId].streamName === streamName){
			return this.netStreams[streamId]; 
			break;
		}
   }
   return null;
};

NetConnection.prototype.deleteNetStreamById = function(streamId){
    this.netStreams[streamId] = null;
};


/**
NetConnection.prototype.setNetStreamName = function(streamId, streamName){
    if(streamId in this.netStreams){
        var ns = this.netStreams[streamId];
        ns.streamName = streamName;
        this.NetStreamNames[streamName] = streamId;
    }else {
        debug(streamId, this.netStreams, (streamId in this.netStreams));
        throw new Error("streamid " + streamId + " not found"); 
    }
};
**/

NetConnection.prototype.setNetStreamById = function(streamId, NetStream){
    if(!(streamId in this.netStreams)){
        this.netStreams[streamId] = NetStream;
    }else {
        throw new Error("streamID " + streamId + " Already exist"); 
    }
};


NetConnection.prototype.acceptPublish = function(streamId){
    (function(){
        var objs = [
        {type: amf0Types.kStringType, value: "onFCPublish"},
        {type: amf0Types.kNumberType, value: 0 },
        {type: amf0Types.kNullType,   value: 0 },
        {type:amf0Types.kObjectType, value: 
            [
            {type:amf0Types.kStringType, key:"level", value:"status"}, 
            {type:amf0Types.kStringType, key:"code", value: "NetStream.Publish.Start"}, 
            {type:amf0Types.kStringType, key:"description", value:"Started publishing stream."}, 
            ]
        }
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg = Buffer.concat(chunks);
       var info = { csid: 5,streamId: streamId , msg:msg  , msgLength:msg.length};
       var buffer = this.encoder.AMF0CmdMessage(info);
       this.socket.write(buffer);
    }.call(this));


    (function(){
        var objs = [
        {type: amf0Types.kStringType, value: "onStatus"},
        {type: amf0Types.kNumberType, value: 0 },
        {type: amf0Types.kNullType,   value: 0 },
        {type:amf0Types.kObjectType, value: 
            [
            {type:amf0Types.kStringType, key:"level", value:"status"}, 
            {type:amf0Types.kStringType, key:"code", value: "NetStream.Publish.Start"}, 
            {type:amf0Types.kStringType, key:"description", value:"Started publishing stream."}, 
            ]
        }
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg = Buffer.concat(chunks);
       var info = { csid: 5,streamId: streamId , msg:msg  , msgLength:msg.length};
       var buffer = this.encoder.AMF0CmdMessage(info);
       this.socket.write(buffer);
    }.call(this));
}

NetConnection.prototype.acceptPlay = function(streamId){
    //(0) SetChunkSize
    this.socket.write(this.encoder.setChunkSize(CONSTS.outChunkSize));
    //(1) stream Begin
    debug("Function[%s] stream Begin[%d]", "acceptPlay", streamId);

    (function(){
        var buffer = this.encoder.UserControlMessageEvents(0x00, streamId);
        debug('UserControlMessages:Stream Begin', buffer);
        this.socket.write(buffer);
    }.call(this));

    //(2) NetStream.Play.Reset
    debug("Function[%s] NetStream.Play.Reset", "acceptPlay");
    (function(){
        var objs = [
        {type: amf0Types.kStringType, value: "onStatus"},
        {type: amf0Types.kNumberType, value: 0 },
        {type: amf0Types.kNullType,   value: 0 },
        {type:amf0Types.kObjectType, value: 
            [
            {type:amf0Types.kStringType, key:"level", value:"status"}, 
            {type:amf0Types.kStringType, key:"code", value: "NetStream.Play.Reset"}, 
            {type:amf0Types.kStringType, key:"description", value:"Playing and resetting stream."}, 
            {type:amf0Types.kStringType, key:"details", value:"stream"}, 
            {type:amf0Types.kStringType, key:"clientid", value:"ASAICiss"}, 
            ]
        }
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg = Buffer.concat(chunks);
       var info = { fmt: 0,  csid: 0x05, msg: msg, msgLength: msg.length, streamId: streamId};
       var buffer = this.encoder.AMF0CmdMessage(info);
       this.socket.write(buffer);
    }.call(this));


    //(3) NetStream.Play.Start
    debug("Function[%s] NetStream.Play.Start", "acceptPlay");
    (function(){
        var objs = [
        {type: amf0Types.kStringType, value: "onStatus"},
        {type: amf0Types.kNumberType, value: 0 },
        {type: amf0Types.kNullType,   value: 0 },
        {type:amf0Types.kObjectType, value: 
            [
            {type:amf0Types.kStringType, key:"level", value:"status"}, 
            {type:amf0Types.kStringType, key:"code", value: "NetStream.Play.Start"}, 
            {type:amf0Types.kStringType, key:"description", value:"NetStream start Play"}, 
            ]
        }
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg = Buffer.concat(chunks);
       var info = { fmt: 0, csid: 0x05, msg: msg, msgLength: msg.length, streamId: streamId};
       var buffer = this.encoder.AMF0CmdMessage(info);
       this.socket.write(buffer);
    }.call(this));

    //(4) |RtmpSampleAccess
    debug("Function[%s] |RtmpSampleAccess", "acceptPlay");
    (function(){
        var objs = [
        {type: amf0Types.kStringType, value: "|RtmpSampleAccess"},
        {type: amf0Types.kBooleanType, value: false},
        {type: amf0Types.kBooleanType, value: false},
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg = Buffer.concat(chunks);
       var info = { fmt: 0, csid: 0x05, msg: msg, msgLength:msg.length, streamId: streamId};
       var buffer = this.encoder.AMF0DataMessage(info);
       this.socket.write(buffer);
    }.call(this));

    //(5) NetStream.Data.Start
    debug("Function[%s] NetStream.Data.Start", "acceptPlay");
    (function(){
        var objs = [
        {type: amf0Types.kStringType, value: "onStatus"},
        {type:amf0Types.kObjectType, value: 
            [
            {type:amf0Types.kStringType, key:"code", value: "NetStream.Data.Start"}, 
            ]
        }
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
       });
       var msg = Buffer.concat(chunks);
       var info = { fmt: 0, csid: 0x05, msg: msg, msgLength: msg.length, streamId: streamId};
       var buffer = this.encoder.AMF0DataMessage(info);
       this.socket.write(buffer);
    }.call(this));
}

NetConnection.prototype._onc0 = function(buf){
    var c0 = buf.readUInt8(0);   
    if(c0 !== 0x03){
        return this.emit('error', new Error('expected RTMP version : 0x03, got: ' + c0));      
    }
    this.onC0S0(buf);
    this._bytes(1536, this._onc1);
};

NetConnection.prototype.onC0S0 = function(c0s0){
    console.log("Function[NetConnection.onC0S0] enter, role", this.role);
    if (this.role === NetConnection.Role.IN){
        this.c0 = c0s0;
        this.emit('c0', c0s0);
    } else {
        this.s0 = c0s0; 
        this.emit('s0', c0s0);
    }
    console.log("Function[NetConnection.onC0S0] leave, role", this.role);
};

NetConnection.prototype._onc1 = function(buf){
    this.onC1S1(buf);
    this._bytes(1536, this._onc2);
};

NetConnection.prototype.onC1S1 = function(c1s1){
    // console.log("Function[NetConnection.onC1S1] enter, c1s1.length: %d, role: %d", c1s1.length, this.role);
    if (this.role === NetConnection.Role.OUT){
        this.s1 = c1s1; 
        this.emit('s1', c1s1);
    } else { // write s0s1s2
        this.c1 = c1s1;
        var self = this;
        var ts   = this.c1.readUInt32BE(0); // timestamp
        var pv   = this.c1.readUInt32BE(4); // peer version
        if(pv === 0){
            this.socket.write(this.c0);
            this.socket.write(new Buffer(1536));
            this.socket.write(this.c1);
        }else{
            handshake.generateS0S1S2(Buffer.concat([this.c0, this.c1]), function(err, s0s1s2, keys){
                if(err){
                    console.log("Functin[NetConnectin.onC1 generate S0S1S2 error", err);
                    return;
                } 
                self.socket.write(s0s1s2);
            });
        }
        this.emit('c1', c1s1);
    }
    // console.log("Function[NetConnection.onC1] leave, c1s1.length: %d, role: %d", c1s1.length, this.role);
};

NetConnection.prototype._onc2 = function(buf){
	assert(buf.length === 1536);
    this.onC2S2(buf);
    this._bytes(1, this._onBasicHdr);       
};

NetConnection.prototype.onC2S2 = function(c2s2){
    // console.log("Function[NetConnection.onC2S2] enter, c2s2.length: %d, role: %d", c2s2.length, this.role);
    if (this.role === NetConnection.Role.OUT){
        this.s2 = c2s2; 
        this.socket.write(this.s1);
        this.emit('s2', c2s2);
    } else {
        this.c2 = c2s2;
        this.emit('c2', c2s2);
    }
    // console.log("Function[NetConnection.onC2S2] enter, c2s2.length: %d, role: %d", c2s2.length, this.role);
};

NetConnection.prototype._onBasicHdr = function(buf){
    debug("Function[_onBasicHdr] cliendId[%s], enter", this.clientId);
    var b    = buf.readUInt8(0);
    var fmt  = b >> 6;
    var csid = b & 0x3f;
    if(fmt === 0 || fmt === 1){
        this.firstFmt = fmt; 
    }
    this.fmt = fmt;
    this.csid = csid;

    /**
    var arg = {'fmt':  fmt, 'csid': csid };
    this.emit('BasicHeader', arg);
    **/

    var size = this.headerSize[fmt];

    debug('Function[_onBasicHdr] clientId[%s],to Read %d bytes, fmt:%d, csid:%d, Buffer:', this.clientId, size, fmt, csid, buf);

    if(csid === 0 || csid === 1){
        this._bytes(csid+1, this._onExtraChunkBasicHdr);       
    }else if(size > 0){
        this._bytes(size, this._onMessageHdr);       
    }else {
        debug("Function[_onBasicHdr] clientId[%s] fmt:3", this.clientId);
        this._onMessageHdr.call(this,new Buffer(0));
    }
    debug("Function[_onBasicHdr] client[%s] leave", this.clientId);
};

NetConnection.prototype._onExtraChunkBasicHdr = function(buf){
    assert(buf.length === this.csid + 1);
    switch(this.csid){
    case 0:
        this.csid = buf.readUInt8(0) + 64;
        break;
    case 1:
        this.csid = buf.readUInt8(1)*256 + buf.readUInt8(0) + 64;
        break;     
    }

    var size = this.headerSize[this.fmt]; 

    debug('Function[_onExtraChunkBasicHdr] clientId[%s], to read size :%d, fmt:%d, csid:%d, Buffer:', this.clientId, size,  this.fmt, this.csid, buf);

    if(size > 0){
        this._bytes(size, this._onMessageHdr);       
    }else {
        this._onMessageHdr.call(this,new Buffer(0));
    }
}
NetConnection.prototype._onMessageHdr = function(buf){
    this.enterCount++;
    debug("Function[_onMessageHdr] clientId[%s], enter", this.clientId);

    debug('Function[_onMessageHdr] clientId[%s], fmt:%d',this.clientId, this.fmt, buf);

    var chunkStream = this.chunkStream[this.csid] ||
    (this.chunkStream[this.csid] = { count: 0, fmt: this.fmt, csid: this.csid,  chunkReadSize: 0, msgLength:0, msgType:0, streamId: 0, timestamp: 0, absTimestamp: 0, msg:null});

	if(chunkStream.count === 0 && this.fmt !== 0){
		debug('Function[_onMessageHdr] clientId[%s], chunkStream is fresh, fmt must be 0, actual is %d, cid is  ',this.clientId, this.fmt, this.csid, buf);
	}

    chunkStream.firstFmt = this.firstFmt;
    chunkStream.fmt = this.fmt;

	var timestamp = 0;
    switch(this.fmt){
    case 0:
        assert(buf.length == 11);
        chunkStream.absTimestamp = readUInt24BE(buf,0);
        chunkStream.msgLength	= readUInt24BE(buf,3);
        chunkStream.msgType		= buf.readUInt8(6);
        chunkStream.streamId	= buf.readUInt32LE(7);
        chunkStream.msg			= new Buffer(chunkStream.msgLength);

		timestamp				= chunkStream.absTimestamp;

        debug('Function[_onMessageHdr] clientId[%s], fmt:%d, timestamp:%d, msgLength:%d, msgType:%d, streamId:%d, Buffer:', this.clientId, this.fmt, chunkStream.absTimestamp, chunkStream.msgLength, chunkStream.msgType, chunkStream.streamId, buf);
        break;
    case 1:
        assert(buf.length == 7);
        chunkStream.timestamp = readUInt24BE(buf,0);
        chunkStream.msgLength = readUInt24BE(buf,3);
        chunkStream.msgType   = buf.readUInt8(6);
        chunkStream.msg       = new Buffer(chunkStream.msgLength);

		timestamp		      = chunkStream.timestamp;
        debug('Function[_onMessageHdr] clientId[%s], fmt:%d, timestamp:%d, msgLength:%d, msgType:%d, Buffer:', this.clientId, this.fmt, chunkStream.timestamp, chunkStream.msgLength, chunkStream.msgType, buf);
        break;
    case 2:
        assert(buf.length == 3);
        chunkStream.timestamp = readUInt24BE(buf,0);
		timestamp = chunkStream.timestamp;
        debug('Function[_onMessageHdr] clientId[%s], fmt:%d, timestamp:%d, Buffer:', this.clientId, this.fmt, chunkStream.timestamp, buf);
        break;
    case 3:
        break; 
    }

    // this.emit('MessageHdr',{fmt: chunkStream.fmt, msgLength: chunkStream.msgLength});

    if( timestamp >= 0xFFFFFF){
        this._bytes(4, this._onExtendTimestamp);
    }else {
		if(this.fmt == 1 || this.fmt == 2){
			chunkStream.absTimestamp += chunkStream.timestamp;	
		}
        debug("Function[_onMessageHdr] clientId[%s] msgLength: %d, chunkStream.chunkReadSize:%d, inChunkSize:%d", this.clientId, chunkStream.msgLength, chunkStream.chunkReadSize, this.inChunkSize);

        var toReadSize = Math.min(chunkStream.msgLength - chunkStream.chunkReadSize, this.inChunkSize);
        debug('Function[_onMessageHdr] clientId[%s] toReadSize: %d msgLength: %d, chunkReadSize:%d', this.clientId, toReadSize, chunkStream.msgLength, chunkStream.chunkReadSize);
        if( toReadSize < this.inChunkSize){
			assert(toReadSize > 0);
            this._bytes(toReadSize, this._onChunkData);       
        }else{
			assert(this.inChunkSize >0);
            this._bytes(this.inChunkSize, this._onChunkData);       
        }
    }
    debug("Function[_onMessageHdr] clientId[%s] leave", this.clientId);
    this.leaveCount++;
};

NetConnection.prototype._onExtendTimestamp = function(buf){

    var time = buf.readUInt32BE(0); 

    var chunkStream = this.chunkStream[this.csid] ;

    if(chunkStream.fmt == 0){
        chunkStream.absTimestamp = time;
    }else if(chunkStream.fmt == 1 || chunkStream.fmt == 2){
        chunkStream.timestamp = time;
		chunkStream.absTimestamp += time;
    }

    debug("Function[_onChunkData] clientId[%s] enter, fmt: %d, time:%d", this.clientId, chunkStream.fmt, time);

    var toReadSize = Math.min(chunkStream.msgLength - chunkStream.chunkReadSize, this.inChunkSize);
    if( toReadSize < this.inChunkSize){
        this._bytes(toReadSize, this._onChunkData);       
    }else{
        this._bytes(this.inChunkSize, this._onChunkData);       
    }
};

NetConnection.prototype._onChunkData = function(buf){
    debug("Function[_onChunkData] clientId[%s] enter", this.clientId);
    
    var chunkStream = this.chunkStream[this.csid]; 

    debug('Function[_onChunkData] clientId[%s] current offset: %d, msgLength: %d, inChunkSize: %d, bufferLength:%d', this.clientId, chunkStream.chunkReadSize, chunkStream.msg.length, this.inChunkSize,buf.length);

    buf.copy(chunkStream.msg, chunkStream.chunkReadSize);
    chunkStream.chunkReadSize += buf.length;

    debug("Function[_onChunkData] clientId[%s] chunkReadSize:%d", this.clientId, chunkStream.chunkReadSize); 

    if(chunkStream.chunkReadSize == chunkStream.msgLength){
        this.onMessage(chunkStream); //call onMessage to handle this
        chunkStream.chunkReadSize = 0;
    }
    debug("Function[_onChunkData] clientId[%s] leave, count: enter %d vs leave %d", this.clientId, this.enterCount, this.leaveCount);
    this._bytes(1, this._onBasicHdr);
};

//(1)
NetConnection.prototype.SetChunkSize = function (chunkStream) {
	debug("Function[SetChunkSize] enter");

    if(chunkStream.msg.length >= 4){
        this.inChunkSize = chunkStream.msg.readUInt32BE(0); 
    }else {
        debug('Function[SetChunkSize], bad payloadSize, chunkStream:', chunkStream);
    }
    debug('Function[SetChunkSize] clientId[%s], inChunkSize:%s',this.clientId, this.inChunkSize);

	debug("Function[SetChunkSize] leave");
};
//(2)
NetConnection.prototype.AbortMessage = function (chunkStream) {
	debug("Function[AbortMessage] enter");

    debug("Function[AbortMessage] clientId[%s]", this.clientId);
    if(chunkStream.msg.length >= 4){
        var chunkStreamId = chunkStream.msg.readUInt32BE(0); 
    }else {
        debug('Function[AbortMessage], bad payloadSize, chunkStream:', chunkStream);
    }

	debug("Function[AbortMessage] leave");
};
//(3)
NetConnection.prototype.Acknowledgement = function (chunkStream) {
	debug("Function[Acknowledgement] enter");

    debug("Function[Acknowledgement] clientId[%s]", this.clientId);
	var ackWindowSize = chunkStream.msg.readUInt32BE(0);
    debug("Function[Acknowledgement] clientId[%s], windowsSize: %d", this.clientId, ackWindowSize);

	debug("Function[Acknowledgement] leave");
};
//(4)
NetConnection.prototype.UserControlMessages = function (chunkStream) {
	debug("Function[UserControlMessage] enter");

    debug("Function[UserControlMessages] clientId[%s]", this.clientId, chunkStream.msg);

    var eventType = chunkStream.msg.readUInt16BE(0);
    switch(eventType){
        case 0: // StreamBegin
            var streamId = chunkStream.msg.readUInt32BE(2);
            var ns = this.findNetStreamById(streamId);
			debug("Function[UserControlMessages] clientId[%s] StreamBegin streamId[%d]", this.clientId, streamId);
            break; 
        case 1: // StreamEOF
            var streamId = chunkStream.msg.readUInt32BE(2);
			debug("Function[UserControlMessages] clientId[%s] StreamEOF streamId[%d]", this.clientId, streamId);
            break;
        case 2: // StreamDry
            var streamId = chunkStream.msg.readUInt32BE(2);
			debug("Function[UserControlMessages] clientId[%s] StreamDry streamId[%d]", this.clientId, streamId);
            break;
        case 3: // SetBufferLength
            var streamId = chunkStream.msg.readUInt32BE(2);
            var bufferLength = chunkStream.msg.readUInt32BE(6);
			debug("Function[UserControlMessages] clientId[%s] SetBufferLength streamId[%d], bufferLength[%d]", this.clientId, streamId, bufferLength);
            break;
        case 4: // StreamIsRecorede
            var streamId = chunkStream.msg.readUInt32BE(2);
			debug("Function[UserControlMessages] clientId[%s] StreamIsRecord streamId[%d]", this.clientId, streamId);
            break;
        case 5: // 
            break;
        case 6: // PingRequest
            var timestamp = chunkStream.msg.readUInt32BE(2);
			debug("Function[UserControlMessages] clientId[%s] PingRequest timestamp[%d]", this.clientId, timestamp);
            break;
        case 7: // PingResponse
            var timestamp = chunkStream.msg.readUInt32BE(2);
			debug("Function[UserControlMessages] clientId[%s] PingResponse timestamp[%d]", this.clientId, timestamp);
            break;
        default:
            break;
    }

	debug("Function[UserControlMessages] leave");
};
//(5)
NetConnection.prototype.WindowAcknowledgementSize = function (chunkStream) {
	debug("Function[WindowAcknowledgementSize] enter");

    debug("Function[WindowAcknowledgementSize] clientId[%s]", this.clientId);
	var AcknowledgementWindowSize = chunkStream.msg.readUInt32BE(0);
    debug("Function[WindowAcknowledgementSize] clientId[%s], WindowAcknowledgementSize: %d", this.clientId, AcknowledgementWindowSize);

	debug("Function[WindowAcknowledgementSize] leave");
};
//(6)
NetConnection.prototype.SetPeerBandwidth = function (chunkStream) {
	debug("Function[SetPeerBandwidth] enter");

    debug("Function[SetPeerBandwidnth] clientId[%s], msg: ", this.clientId, chunkStream.msg);
	var acknowledgementWindowSize = chunkStream.msg.readUInt32BE(0);
	var limitType = chunkStream.msg.readUInt8(4);
    debug("Function[SetPeerBandwidnth] clientId[%s], AcknowledgementWindowSize: %d, limitType: %d", this.clientId, acknowledgementWindowSize, limitType);

	debug("Function[SetPeerBandwidth] leave");
};

//(8)
NetConnection.prototype.AudioMessage = function (chunkStream) {
	debug("Function[AudioMessage] enter");

    debug("Function[AudioMessage] clientId[%s]", this.clientId);
    assert(chunkStream.streamId > 0);

    var ns = this.findNetStreamById(chunkStream.streamId);
    var opts = {
        fmt:            chunkStream.firstFmt,
        csid:           chunkStream.csid,
        type:           chunkStream.msgType,
        length:         chunkStream.msgLength,
        timestamp:      chunkStream.firstFmt != 0 ? chunkStream.timestamp : 0,
		absTimestamp:	chunkStream.firstFmt == 0 ? chunkStream.absTimestamp :	0,
		streamId	:	chunkStream.streamId || 0,
        isKeyFrame:     false,
        rawData :       chunkStream.msg,
        body:           this.subscribeEncoder.makeRTMPBody({rawData: chunkStream.msg}),
    };

    debug("Function[AudioMessage] clientId[%s] timestamp: %d", this.clientId, chunkStream.timestamp);

    var Msg = new Message(opts);
    ns.recvAudio(Msg);

    // copy to all subscriber
	/***
    for(var i=0; i < ns.pipe.subscribers.length; i++){
       var sub = ns.pipe.subscribers[i];  
       sub.recvAudio(Msg);
    }
	**/

	debug("Function[AudioMessage] leave");
};
//(9) type == 0x09 
NetConnection.prototype.VideoMessage = function (chunkStream) {
	debug("Function[VideoMessage] enter");

    debug("Function[VideoMessage] clientId[%s]", this.clientId);

    if(!isAvcSeqHdr(chunkStream.msg) && chunkStream.msg.length > 5){
      //splitNALU(this.msg.slice(5));   
    }

    assert(chunkStream.streamId > 0);
    var ns = this.findNetStreamById(chunkStream.streamId);
    var opts = {
        fmt:            chunkStream.firstFmt,
        csid:           chunkStream.csid,
        type:           chunkStream.msgType,
        length:         chunkStream.msgLength,
        timestamp:      chunkStream.firstFmt != 0 ? chunkStream.timestamp	 : 0,
		absTimestamp:	chunkStream.firstFmt == 0 ? chunkStream.absTimestamp : 0,
		streamId:		chunkStream.streamId || 0,
        isKeyFrame:     chunkStream.msgType === 9 && (chunkStream.msg[0]>>4) == 0x01,
        rawData:        chunkStream.msg,
        body:           this.subscribeEncoder.makeRTMPBody({rawData: chunkStream.msg}),
    };
    debug("Function[VideoMessage] called", opts, ns.subscribers.length);
    var Msg = new Message(opts);
    ns.recvVideo(Msg);

    // copy to all subscriber
	/***
    for(var i=0; i < ns.subscribers.length; i++){
       var sub = ns.pipe.subscribers[i];  
       sub.recvVideo(Msg);
    }
	***/

	debug("Function[VideoMessage] leave");
}; 
//(15)
NetConnection.prototype.AMF3DataMessage= function (chunkStream) {
	debug("Function[AMF3DataMessage] enter");
	debug("Function[AMF3DataMessage] leave");
};
//(16)
NetConnection.prototype.AMF3SharedObjectMessage = function (chunkStream) {
	debug("Function[AMF3SharedObjectMessage] enter");
	debug("Function[AMF3SharedObjectMessage] leave");
};
//(17)
NetConnection.prototype.AMF3CmdMessage = function (chunkStream) {
	debug("Function[AMF3CmdMessage] enter");
	debug("Function[AMF3CmdMessage] leave");
};
//(18)
NetConnection.prototype.AMF0DataMessage= function (chunkStream) {
	debug("Function[AMF0DataMessage] enter");

    debug("Function[AMF0DataMessage] clientId[%s]", this.clientId);
    try{
       var position = {offset: 0};         
       var args = [];
       while(position.offset < chunkStream.msgLength){
            var ext = amf.read(chunkStream.msg, position);
            args.push(ext);
       }
       debug("Function[%s] streamId[%d]", 'AMF0DataMessage', chunkStream.streamId, args, chunkStream.msg);
       var cmdName = args[0];
       switch(cmdName){
       case '@setDataFrame':
		   debug("Function[AMF0DataMessage] @setDataFrame");
           var ns = this.findNetStreamById(chunkStream.streamId);
           ns.emit('metadata', args.slice(1));
           var opts = {
                fmt:            chunkStream.fmt,
                type:           chunkStream.msgType,
                length:         chunkStream.msgLength-16,
                timestamp:      chunkStream.timestamp,
                isKeyFrame:     chunkStream.msgType === 9 && (chunkStream.msg[0]>>4) == 0x01 ,
                rawData:        chunkStream.msg.slice(16)
           };
           var Msg = new Message(opts);
           ns.recvMetadata(Msg);

          break;
		case 'onStatus':
		   debug("Function[AMF0DataMessage] onStatus");
			(function(){
				switch (args[1].code){
				case 'NetStream.Data.Start':
				  break;
				default:
				  break;
				}
			}).bind(this).call();
       default:
         break; 
       }
    }catch(e){
        debug('Function[AMF0DataMessage] exception', e);
    }

	debug("Function[AMF0DataMessage] leave");
};
//(19)
NetConnection.prototype.AMF0SharedObjectMessage = function (chunkStream){
	debug("Function[AMF0SharedObjectMessage] enter");
	debug("Function[AMF0SharedObjectMessage] leave");
};
//(20)
NetConnection.prototype.AMF0CmdMessage = function (chunkStream) {
	debug("Function[AMF0CmdMessage] enter");

    debug("Function[AMF0CmdMessage] clientId[%s] MsgLength", this.clientId, chunkStream.msg.length);
    try{
       var position = {offset: 0};
       var args = [];
       while(position.offset < chunkStream.msgLength){
            var ext = amf.read(chunkStream.msg, position);
            args.push(ext);
       }
       var cmdName = args[0];
       var transId = args[1];

       debug('Function[AMF0CmdMessage] clientId[%s], command[%s], argument', this.clientId, cmdName, args);
       switch(cmdName){
           //NetConn CMD
           case 'connect':
               this.emit('connect', args);
               break;
           case 'close':
               this.emit('close', args);
               break;
           case 'createStream':
               this.onCreateStream(args);
               break;
           case '_checkbw':
               // ignore
               break;
           case 'play':
               var streamName = args[3];
               console.log("play command, streamID[%d]", chunkStream.streamId, streamName);
               var ns = this.findNetStreamById(chunkStream.streamId);
               ns.onPlay(args);
               break;
           case 'play2':
               var ns = this.findNetStreamById(chunkStream.streamId);
               ns.onPlay2(args);
               break;
           case 'deleteStream':
               this.onDeleteStream(args[3]);
               /**
                 var ns = this.findNetStreamById(args[3]);
                 ns.clearRole();
                 this.emit(args[0], chunkStream.streamId, args);
                **/
               break;
           case 'closeStream':
               var ns = this.findNetStreamById(chunkStream.streamId);
               if(ns){
                   ns.onClose(args);
                   /**
                     ns.clearRole();
                     ns.emit('closeStream');
                    **/
               }else {
                   debug("Function");
               }
               break;
           case 'receiveAudio':
               this.emit(args[0], chunkStream.streamId, args);
               break;
           case 'receiveVideo':
               this.emit(args[0], chunkStream.streamId, args);
               break;
           case 'publish':
               // publish stream
               var ns = this.findNetStreamById(chunkStream.streamId);
               ns.emit("publish", args);
               break;
           case 'seek':
               this.emit(args[0], chunkStream.streamId, args);
               break;
           case 'pause':
               var ns = this.findNetStreamById(chunkStream.streamId);
               debug('Function[AMF0CmdMessage] pause');
           if(ns){
                var toggle          = args[3];
                var milliSeconds    = args[4];
                ns.pause(toggle, milliSeconds);
                this.emit(args[0], chunkStream.streamId, args);
           }
           break;
       case 'releaseStream':
           this.onReleaseStream(args);
           break;
       case 'FCPublish':
           this.onFCPublish(args);
           break;
	   case 'FCUnpublish':
		   var streamName = args[3];
		   var stream = this.findNetStreamByName(streamName);
		   if (stream) {
			   stream.emit('FCUnpublish'); 
               // response onFCUnpublish
               (function(){
                   var objs = [
                       {type: amf0Types.kStringType, value: "onFCUnpublish"},
                       {type: amf0Types.kNumberType, value: 0 },
                       {type: amf0Types.kNullType,   value: null},
                       {type: amf0Types.kObjectType, value: 
                           [
                           {type:amf0Types.kStringType,  key:"code",   value: "NetStream.Unpublish.Success"},
                           {type:amf0Types.kStringType,  key:"description",  value: streamName || ""},
                           {type:amf0Types.kStringType,  key:"clientId", value: this.clientId}
                           ]
                       },
                   ];
                   var chunks = [];
                   objs.forEach(function(val, idx, arr){
                       chunks.push(amf.write(val));        
                   });
                   var msg = Buffer.concat(chunks);
                   var info = { fmt: 0, csid: 3, msg: msg, msgLength: msg.length, streamId: strea.streamId};
                   buffer = this.encoder.AMF0CmdMessage(info);
                   this.socket.write(buffer);
               
               });
               // response onStatus
               (function(){
                   var objs = [
                       {type: amf0Types.kStringType, value: "onStatus"},
                       {type: amf0Types.kNumberType, value: 0 },
                       {type: amf0Types.kNullType,   value: null},
                       {type: amf0Types.kObjectType, value: 
                           [
                            { type:amf0Types.kStringType,  key:"level",  value: "status"}, 
                            { type:amf0Types.kStringType,  key:"code",   value: "NetStream.Unpublish.Success"},
                            { type:amf0Types.kStringType,  key:"description",  value: streamName || ""},
                            { type:amf0Types.kStringType,  key:"clientId", value: this.clientId}
                           ]
                       },
                   ];
                   var chunks = [];
                   objs.forEach(function(val, idx, arr){
                       chunks.push(amf.write(val));        
                   });
                   var msg = Buffer.concat(chunks);
                   var info = { fmt: 1, csid: 3, msg: msg, msgLength: msg.length};
                   buffer = this.encoder.AMF0CmdMessage(info);
                   this.socket.write(buffer);
               
               });
               (function(){
                   var objs = [
                       {type: amf0Types.kStringType, value: "_result"},
                       {type: amf0Types.kNumberType, value: transId },
                       {type: amf0Types.kNullType,   value: null},
                       {type: amf0Types.kUndefinedType, value: undefined},
                   ];
                   var chunks = [];
                   objs.forEach(function(val, idx, arr){
                       chunks.push(amf.write(val));        
                   });
                   var msg = Buffer.concat(chunks);
                   var info = { fmt: 0, csid: 3, msg: msg, msgLength: msg.length, streamId: stream.streamId};
                   buffer = this.encoder.AMF0CmdMessage(info);
                   this.socket.write(buffer);
               }).call(this);
		   }
		   break;
       case '_result':
           var cb = this.stub[transId];
           if(cb){
                debug("_result argument", args);
				console.log("_result argument", JSON.stringify(args));
                cb(null, args[3]);
           }
           break;
       case 'onStatus':
           debug('last streamID', chunkStream.streamId);
           // (1) find NetStream
           var ns = this.findNetStreamById(chunkStream.streamId);
           if(ns){
				ns.emit('onStatus', args[3]); 
				var pool = this.server.publishPool;
				pool.emit("onStatus", args[3]);
           }
           break;
       case 'call':
           this.emit(args[0], chunkStream.streamId, args);
           break;
       default:
           this.emit(cmdName,args.slice(3), transId);
           break;
       }
    }catch(e){
        debug('Function[AMF0CmdMessage] exception', e, position, args, this);
    }
	debug("Function[AMF0CmdMessage] leave");
};
//(22)
NetConnection.prototype.AggregateMessage = function (chunkStream) {
	debug("Function[Aggregate] enter");
	debug("Function[Aggregate] leave");
};

NetConnection.prototype.call = function Call(cmdName){

    debug("NetConnection.call", arguments);
    var args = Array.prototype.slice.call(arguments);

    var transId = this.nextTransId++;
    var self = this;
    var cb   = args[args.length - 1]
    if(typeof cb === 'function'){
        args.pop();
        this.stub[transId] = cb;
    }

    (function(){
     var objs = [
     {type: amf0Types.kStringType, value: cmdName},
     {type: amf0Types.kNumberType, value: transId},
     {type: amf0Types.kNullType,   value: null},
     ];
     
     for(var i = 1; i < args.length; i++){
        var value = AMF0Serialize( args[i] );
        objs.push(AMF0Serialize(args[i]));
     }

     /**
     for(var i = 1; i < args.length; i++){
        var arg = args[i];  
        var type = typeof arg;
        switch(type){
        case 'boolean': 
            objs.push( {type:amf0Types.kBooleanType, value: arg});
            break;
        case 'number':
            objs.push( {type:amf0Types.kNumberType, value: arg});
            break;
        case 'string':
            logger.debug('push string',{type:amf0Types.kStringType, value: arg});
            objs.push({type:amf0Types.kStringType, value: arg});
            break;
        default:
            break;
        }
     }
     **/
     debug(objs);

     var chunks = [];
     objs.forEach(function(val, idx, arr){
             debug(val);
             chunks.push(amf.write(val));        
             });
     var msg  = Buffer.concat(chunks);
     var info = {fmt:0, csid:3, streamId: 0, msg:msg  , msgLength:msg.length};
     var buffer = self.encoder.AMF0CmdMessage(info);
     self.socket.write(buffer);
    }.call(this));
};

NetConnection.prototype.Response = function Response(success, transId){

    debug("NetConnection.Response", arguments);
    var args = Array.prototype.slice.call(arguments);

    var self = this;

    (function(){
     var objs = [
     {type: amf0Types.kStringType, value: success ? '_result' : '_error'},
     {type: amf0Types.kNumberType, value: transId},
     {type: amf0Types.kNullType,   value: null},
     ];

     for(var i = 2; i < args.length; i++){
        var arg = args[i];  
        var type = typeof arg;
        switch(type){
        case 'boolean': 
            objs.push( {type:amf0Types.kBooleanType, value: arg});
            break;
        case 'number':
            objs.push( {type:amf0Types.kNumberType, value: arg});
            break;
        case 'string':
            debug('push string',{type:amf0Types.kStringType, value: arg});
            objs.push({type:amf0Types.kStringType, value: arg});
            break;
        default:
            break;
        }
     }
     debug(objs);

     var chunks = [];
     objs.forEach(function(val, idx, arr){
             debug(val);
             chunks.push(amf.write(val));        
             });
     var msg  = Buffer.concat(chunks);
     var info = {fmt:0, csid:3, streamId: 0, msg:msg  , msgLength:msg.length};
     var buffer = self.encoder.AMF0CmdMessage(info);
     self.socket.write(buffer);
    }.call(this));
};

NetConnection.prototype.FunctionNames = {
    1: "SetChunkSize",
    2: "AbortMessage",
    3: "Acknowledgement",
    4: "UserControlMessage",
    5: "WindowAcknowledgementSize",
    6: "SetPeerBandwidht",
    7: "Edge",
    8: "Audio",
    9: "Video",
    0x0f: "AMF3DataMessage",
    0x10: "AMF3SharedObjectMessage",
    0x11: "AMF3CmdMessage",
    0x12: "AMF0DataMessage",
    0x13: "AMF0SharedObjectMessage",
    0x14: "AMF0CmdMessage",
    0x16: "AggregateMessage"
};

NetConnection.prototype.onMessage = function (chunkStream) {
    assert(chunkStream);

    debug("Function[onMessage] clientId[%s] msgType:%d", this.clientId, chunkStream.msgType);
    switch(chunkStream.msgType){
    case 0x01:
        // HandleChangeChunkSize
        this.SetChunkSize(chunkStream);
        break;
    case 0x02:
       //RTMP abort Message
       this.AbortMessage(chunkStream);
       break;
    case 0x03:
        /* bytes read report*/
        this.Acknowledgement(chunkStream);
        break;
    case 0x04:
        /* ctrl */
        //HandleCtrl*(;
        this.UserControlMessages(chunkStream);
        break;
    case 0x05:
        /* server bw */
        //HandleServerBW
        this.WindowAcknowledgementSize(chunkStream)
        break;
    case 0x06:
        /* client BW */
        //HandleClientBW
        this.SetPeerBandwidth(chunkStream);
        break;
    case 0x07:
        /* Edge */
        //HandleEdge
		break;
    case 0x08:
        // Audio Data
        //HandleAudio();
        this.AudioMessage(chunkStream);
        break;
    case 0x09:
        // Video Data
        // HandleVideo
        this.VideoMessage(chunkStream);
		break;
    case 0x0F:
        /* Flex stream send */
        this.AMF3DataMessage(chunkStream);
        break;
    case 0x10:
        // Flex shared object
        this.AMF3SharedObjectMessage(chunkStream);
        break;
    case 0x11:
        // Flex Message
        //this.AMF3CmdMessage();
        this.AMF3CmdMessage(chunkStream);
        break;
    case 0x12:
        //HandleMetadata();
        this.AMF0DataMessage(chunkStream);
        break;
    case 0x13:
        // share object
        this.AMF0SharedObjectMessage(chunkStream);
        break;
    case 0x14:
        /* HandleIvoke*/
        this.AMF0CmdMessage(chunkStream);
		break;
    case 0x15:
        break;
    case 0x16:
        this.AggregateMessage(chunkStream);
        break;
    default:
        debug("Function[onMessage], unknow msgType", chunkStream.msgType);
        break;
    }
};

NetConnection.prototype.acceptConnect = function(){
       debug('Function[NetConnection.accept] Accept');
       // Window Ack Size
       var buffer = this.encoder.WindowAcknowledgementSize(250*10000);
       this.socket.write(buffer);

       // Set Peer Bandwidth 
       buffer = this.encoder.SetPeerBandwidth(250*10000,2);
       this.socket.write(buffer);
       var transId = this.nextTransId++;

       // _result('NetConnection.Connect.Success')
       (function(){
           var objs = [
                {type: amf0Types.kStringType, value: "_result"},
                {type: amf0Types.kNumberType, value: transId},
                {type: amf0Types.kObjectType, value: 
                    [
                    {type:amf0Types.kStringType, key:"fmsVer", value:"FMS/3,5,3,888"}, 
                    {type:amf0Types.kNumberType, key:"capabilities", value: 127},
                    {type:amf0Types.kNumberType, key:"mode", value: 1},
                    ]
                },
                {type:amf0Types.kObjectType, value: 
                    [
                    {type:amf0Types.kStringType, key:"level", value:"status"}, 
                    {type:amf0Types.kStringType, key:"code", value:"NetConnection.Connect.Success"}, 
                    {type:amf0Types.kStringType, key:"description", value:"Connection succeeded"}, 
                    {type:amf0Types.kNumberType, key:"objectEncoding", value: 0},
                    ]
                }
            ];
           var chunks = [];
           objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
           });
           var msg = Buffer.concat(chunks);
           var info = { fmt: 0, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
           buffer = this.encoder.AMF0CmdMessage(info);
           this.socket.write(buffer);
       }.call(this));

       // onBWDone
       (function(){
           var objs = [
            {type:amf0Types.kStringType, value:"onBWDone"},
            {type:amf0Types.kNumberType, value:0},
            {type:amf0Types.kNullType, value: undefined}
            ];
            var chunks = [];
            objs.forEach(function(val, idx, arr){
                   chunks.push(amf.write(val));        
            });
            var msg = Buffer.concat(chunks);
            var info = { fmt: 0, csid: 3, msg: msg, msgLength: msg.length, streamId: 0};
            var buffer = this.encoder.AMF0CmdMessage(info);
            this.socket.write(buffer);
       }.call(this));

       // SetChunkSize
       this.socket.write(this.encoder.setChunkSize(60000));
};

NetConnection.prototype.rejectConnect = function(){
       debug('Function[NetConnection.rejectConnect]');

       // NetConnection.Connect.Reject
       (function(){
        var objs = [
            {type: amf0Types.kStringType, value: "_error"},
            {type: amf0Types.kNumberType, value: 1 },
            {type: amf0Types.kNullType,   value: null},
            {type:amf0Types.kObjectType, value: 
                [ {type:amf0Types.kStringType, key:"level", value:"error"}, 
                  {type:amf0Types.kStringType, key:"code", value:"NetConnection.Connect.Rejected"}, 
                  {type:amf0Types.kStringType, key:"description", value:"Connection Rejected"}, 
                  {type:amf0Types.kNumberType, key:"objectEncoding", value: 0},
                ]
            }
       ];
       var chunks = [];
       objs.forEach(function(val, idx, arr){
               chunks.push(amf.write(val));        
        });
       var msg = Buffer.concat(chunks);
       var info = {msg:msg  , msgLength:msg.length, streamId:0};
       buffer = this.encoder.AMF0CmdMessage(info);
       this.socket.write(buffer);
       }.call(this));

       // close the Netconnection
       (function(){
           var objs = [
                {type:amf0Types.kStringType, value:"close"},
                {type:amf0Types.kNumberType, value:0},
                {type:amf0Types.kNullType, value: undefined}
            ];
            var chunks = [];
            objs.forEach(function(val, idx, arr){
                   chunks.push(amf.write(val));        
            });
            var msg = Buffer.concat(chunks);
            var info = {msg:msg  , msgLength:msg.length, streamId:0};
            var buffer = this.encoder.AMF0CmdMessage(info);
            this.socket.write(buffer);
       }.call(this));

        this.socket.write(this.encoder.setChunkSize(60000));
};

NetConnection.prototype.accept = NetConnection.prototype.acceptConnect;

NetConnection.prototype.reject = NetConnection.prototype.rejectConnect;


/**
 * Called for the NetConnection's "finish" event. Pushes the `null` packet to the audio
 * and/or video stream in the FLV file, so that they emit "end".
 *
 * @api private
 */

NetConnection.prototype._onfinish = function () {
  /*
  this._streams.forEach(function (stream) {
    stream._pushAndWait(null, function () {
      debug('`null` packet flushed');
    });
  });
  this._streams.splice(0); // empty
  */
};

/**
 * forward the stream to others servers
 *
 * INIT PUBLISH
 */
function EdgePublish(opts){
	if (!(this instanceof EdgePublish)) {
		return new EdgePublish(opts);
	}

	this.state = EdgePublish.STATE.INIT;

	this.server = opts.server;
	this.pipe   = opts.pipe;

	this.metadata = null;
	this.audioSeqHdr = null;
	this.videoSeqHdr = null;

	this.remoteHost = null;
	this.remotePort = null;
	this.remoetApp  = 'live';

	this.queue = [];
}
inherits(EdgePublish, require('events').EventEmitter);

EdgePublish.STATE = {
	INIT		: 0x01,
	PUBLISHED	: 0x02,
};

/**
 * @param: vhost,
 * @param: host,
 * @param: port,
 * @param: app,
 * @param: streamName,
 * @param: source {NetStream},
 */
EdgePublish.prototype.connect = function (opts) {
    console.log("Function[EdgePublish] connect ");

	var self = this;
	this.host	= opts.host;
	this.port	= opts.port;

	this.on('publish', EdgePublish.prototype.onPublish.bind(this));
	this.on('unpublish', EdgePublish.prototype.onUnpublish.bind(this));

	this.forwarder = new NetConnection({_server: self.server, _socket: null, _role: NetConnection.Role.OUT});
	// connect server
	this.forwarder.connect({vhost: this.pipe.vhost, host: opts.host, port: opts.port, app: this.pipe.app} , function (err, event){
		console.log("Function[EdgePublish-forwarder] err", err, JSON.stringify(event));
		if (err) {
			return;	
		}
		switch(event.code){
			case 'NetConnection.Connect.Success':
				self.forwarder.fmleCreateStream(self.pipe.streamName, function(err, ns){
					console.log("Function[EdgePublish-forwarder-createStream] err", err);
					self.forwarder.emit('__createStream', ns);
				});
				break;	
		}
	});
	this.forwarder.on('__createStream', function (stream){
		console.log("Function[__createStream] publish, streamName", self.pipe.streamName);
		stream.publish(self.pipe.streamName, function (err){
			if(err) {
				return;	
			}
		});
		stream.on('onStatus', function(event){
			console.log("Function[EdgePublish-onStatus] onStatus", JSON.stringify(event));
			if(!event){
				return;	
			}
			switch(event.code){
				case 'NetStream.Publish.Start':
					//self.emit('publish', stream);
					self.pipe.emit('play', stream);
					break;	
				case 'NetStream.Unpublish.Success':
					self.pipe.emit('stopPlay', stream);
					break;
				default:
				break
			}
		});
		stream.on("unpublish", function(){
			console.log("Function[EdgePublish] on-unPublish");
			self.emit('stopPlay', stream);
		});
		stream.on('closeStream', function (){
			console.log("Function[EdgePublish] on-publish-closeStream");
			self.emit('stopPlay', stream);
		});
	});
};

EdgePublish.prototype.onPublish		= function(stream){
	this.stream = stream;
	this.state = EdgePublish.STATE.PUBLISHED;
	console.log("EdgePublish onPublish", this.state);
	this.pipe.appendSubscriber(stream);
};

EdgePublish.prototype.onUnpublish	= function(stream){
	console.log("EdgePublish onUnpublish");
	this.state = EdgePublish.STATE.INIT;
};

EdgePublish.prototype.onMetaData	= function(metadata){
	console.log("EdgePublish onMetaData");
	this.metadata = metadata;	
};

EdgePublish.prototype.onAacSeqHdr	= function (aacSeqHdr){
	console.log("EdgePublish onAacSeqHdr");
	this.aacSeqHdr = aacSeqHdr;
};

EdgePublish.prototype.onAvcSeqHdr	= function (avcSeqHdr){
	console.log("EdgePublish onAvcSeqHdr");
	this.avcSeqHdr = avcSeqHdr;
};

EdgePublish.prototype.onAudio		= function(audidoMsg){
};

EdgePublish.prototype.onVideo		= function(videoMsg){};

EdgePublish.prototype.onClose = function(){};


/**
 * Ingest stream from Origin Server
 */
function EdgePlay(opts)
{
	if (!(this instanceof EdgePlay)) {
		return new EdgePlay(opts);	
	}

	this.state	= EdgePlay.STATE.INIT;
	this.server = opts.server;
	this.pipe	= opts.pipe;
	this.vhost	= opts.vhost;
	this.app	= opts.app;
	this.streamName = opts.streamName;
	this.host	= null;
	this.port	= null;
	this.ingester = null;

}
inherits(EdgePlay, require('events').EventEmitter);

EdgePlay.STATE = {
	INIT	: 0x01,
	PLAY	: 0x02,
};

/**
 * @param: server {RTMPServer}
 * @param: host {String}
 * @param: port {Number}
 * @param: app	{String}
 * @param: streamName {String} 
 * @param: 
 */

EdgePlay.prototype.connect = function(opts){
	console.log("Function[EdgePlay.connect] enter, opts: %s", JSON.stringify(opts));

	var self		= this;
	this.host		= opts.host;
	this.port		= opts.port;

	opts.vhost		= this.vhost;
	opts.app		= this.app;
	opts.streamName	= this.streamName;

	var nc = this.ingester = new NetConnection({_server: this.server, _socket: null, _role: NetConnection.Role.OUT});
	nc.connect(opts, function(err, event) {
		if (err) {
			console.log("Function[EdgePlay.connect] err", err);
			return;
		}
		console.log("Function[EdgePlay.connect] callback", JSON.stringify(event));
		switch (event.code){
			case 'NetConnection.Connect.Failed':
				break;
			case 'NetConnection.Connect.Success':
				console.log("Function[EdgePlay.createStream]");
				nc.createStream(function(err, stream){
					console.log("Function[EdgePlay.createStream] err", err);
					if (err) {
						return;
					}
					stream.play(self.streamName, function (err){
						if (err) {
							console.log("Function[EdgePlay.connect.stream.play] err", err);
						}
						//stream.setFlags(stream.Flags.PIPE);
					});
					stream.on('onStatus', function (event){
						console.log("Function[EdgePlay.connect.stream.onStatus] ", JSON.stringify(event));
						switch (event.code){
							case 'NetStream.Play.StreamNotFound':
								break;
							case 'NetStream.Play.Reset':
								break;
							case 'NetStream.Play.Start':
								self.pipe.emit("publish", null, stream);
								break;
							case 'NetStream.Play.Stop':
								break;
							case 'NetStream.Play.Completed':
								break;
							case 'NetStream.Play.UnpublishNotify':
								break;
							default:
								break;
						}
					});
					stream.on('metadata', function(metadata){
                        console.log("Function[EdgePlay.connect.stream] onMetadata");
						self.pipe.onMetaData(metadata);	
					});
					stream.on('aacSeqHdr', function(aacSeqHdr){
                        console.log("Function[EdgePlay.connect.stream] onAacSeqHdr");
						self.pipe.onAacSeqHdr(aacSeqHdr);
					});
					stream.on('avcSeqHdr', function(avcSeqHdr){
                        console.log("Function[EdgePlay.connect.stream] onAvcSeqHdr");
						self.pipe.onAvcSeqHdr(avcSeqHdr);
					});
					stream.on('audio', function(msg){
						self.pipe.onAudio(msg);	
					});
					stream.on('video', function(msg){
						self.pipe.onVideo(msg);	
					});
				});
				break;	
		}
	});
	console.log("Function[EdgePlay.connect] leave, opts: %s", JSON.stringify(opts));
};

/**
 * Node.js Buffer class doesn't have readUInt24...
 */

function readUInt24BE (buffer, offset) {
  var val = 0;
  val |= buffer[offset + 2];
  val |= buffer[offset + 1] << 8;
  val |= buffer[offset + 0] << 16;
  return val;
}

function writeUInt24BE (buffer, offset, val) {
  buffer[offset++] = (val >> 16) & 0xff
  buffer[offset++] = (val >>  8) & 0xff
  buffer[offset++] = (val >>  0) & 0xff
}

function readBits (x, length) {
  return (x >> (32 - length));
}

function writeBits (x, length, value) {
  var mask = 0xffffffff >> (32 - length);
  x = (x << length) | (value & mask);
  return x;
}

/**
 * tagType: UI8  0x8 | 0x9 | 0x12
 * dataSize: UI24
 * timestamp: UI32
 * data: xxx
 **/
function MakeTag(opts){
    // TagHeader + data + prevTagSize
    var tag = new Buffer(11);
    var offset = 0;
    // TagType UI8
    tag.writeUInt8(opts.tagType, offset);
    offset += 1;

    // DataSize UI24
    writeUInt24BE(tag, offset, opts.data.length);
    offset += 3;

    // Timestamp UI24
    writeUInt24BE(tag, offset, opts.timestamp);
    offset += 3;

    // TimestampExtended UI8
    tag.writeUInt8(opts.timestamp >> 24, offset);
    offset += 1;

    // StreamID UI24
    writeUInt24BE(tag, offset, 0);
    offset += 3;

    var prevTagSize = new Buffer(4);
    prevTagSize.writeUInt32BE(offset+opts.data.length, 0);

    return Buffer.concat([tag, opts.data, prevTagSize]);
}

/**
 * @brief: AVCDecoderConfigurationRecord
 * @param: [in] 
 * {
 *  configVersion: UI8
 *  AVCProfileIdx :  UI8
 *  profile_compatibility:  UI8
 *  AVCLevelIdx:   UI8
 *  NALUSize   :   UI8
 *  numOfSPS   :   UI8
 *      SPSLength: UI16
 *      SPSNALU:
 *  numOfPPS
 *      PPSLength: UI16
 *      PPSNALU:
 * }
 */
function AVCDecoderConfigurationRecord(buff)
{
   assert(buff.length >= 10); 
   var offset = 0;
   // (1) configurationVersion
   var configVersion = buff[offset++];
   // (2) AVCProfileIndication
   var AVCProfileIdx    = buff[offset++];
   // (3) profile_compatibility
   var profile_compatibility    = buff[offset++];
   // (4) AVCLevelIndication
   var AVCLevelIdx   = buff[offset++];
   // (5) lengthSizeMinusOne
   var NALUSize = 1 + (buff[offset++]&0x3);


   var spsArr = [];
   // (6) numOfSequenceParameterSetsLength
   var numOfSPS = buff[offset++] & 0x1F;
   for(var i=0; i < numOfSPS; i++){
       // (7) sequenceParameterSetLength
        var len = buff.readUInt16BE(offset);
        offset += 2;
       // (8) sequeneceParamerterSetNALUnit
        var sps = new Buffer(len);
        buff.copy(sps,0,offset,offset+len);
        offset += len;
        spsArr.push(sps);
   }

   var ppsArr = [];
   // (9) numOfPictureParameterSets
   var numOfPPS = buff[offset++];
   for(var i=0; i < numOfPPS; i++){
        // (10) pictureParameterSetLength 
        var len = buff.readUInt16BE(offset);
        offset += 2;
        var pps = new Buffer(len);
        buff.copy(pps,0,offset,offset+len);
        offset += len;
        ppsArr.push(pps);
   }
   return {
    configVersion: configVersion,
    AVCProfileIdx: AVCProfileIdx,
    profile_compatibility:  profile_compatibility,
    AVCLevelIdx  : AVCLevelIdx,
    NALUSize     : NALUSize,
    numOfSPS     : spsArr.length,
    sps          : spsArr,
    numOfPPS     : ppsArr.length,
    pps          : ppsArr 
   };
}

/**
 * audioObjectType : 5bit
 */
function AudioSpecificConfig(buff)
{
    var audioObjectType = buff[0] >> 3;    
    var samplingFrequencyIndex = ((buff[0] & 0x7) << 1) | (buff[1] >> 7) 
    var channelConfiguration = (buff[1]>>3)&0x0F
    var frameLengthFlag      = (buff[1] >> 2) & 0x01
    var dependsOnCoreCoder   = (buff[1] >> 1) & 0x01
    var extensionFlag        =  buff[1] & 0x01;

    return {
        audioObjectType:    audioObjectType,
        samplingFrequencyIndex: samplingFrequencyIndex,
        channelConfiguration  : channelConfiguration,
        frameLengthFlag       : frameLengthFlag,
        dependsOnCoreCoder    : dependsOnCoreCoder,
        extensionFlag         : extensionFlag
    };
}

function splitNALU(buff){
    var offset     = 0;
    var naluLength = 0;
    var buffLength = buff.length;
    do{
        naluLength = buff.readUInt32BE(offset);
        buff.writeUInt32BE(0x01, offset);
        offset += 4;
        offset += naluLength;
    }while(offset + 4 < buffLength);
}

function generateClientID()
{
    var possibile = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456";
    var numPossible = possibile.length;
    var clientID    = Math.floor(Date.now()/1000);

    for(var i=0; i < 8; i++){
        clientID += possibile.charAt((Math.random()*numPossible | 0)) 
    }
    return clientID;
}

function generateNewClientID(sessions)
{
   var clientID = generateClientID(); 
   while(clientID in sessions){
        clientID    = generateClientID(); 
   }
   return clientID;
}

function AMF0Type(obj){
    var type = typeof obj;
    switch(type){
    case 'number':
       return amf0Types.kNumberType; 
    case 'boolean':
       return amf0Types.kBooleanType; 
    case 'string':
       return amf0Types.kStringType; 
    case 'object':
       if(obj === undefined){
           return amf0Types.kUndefinedType; 
       }
       if(obj === null){
           return amf0Types.kNullType; 
       }
       if(Array.isArray(obj)){
           return amf0Types.kStrictArrayType;
       }
       return amf0Types.kObjectType;
    case 'null':
       return amf0Types.kNullType; 
    case 'undefined':
        return amf0Types.kUndefinedType; 
    default:
       return null;
    }
}

function AMF0Serialize(obj){

   var type = AMF0Type(obj); 
   switch(type){
   case amf0Types.kNumberType:
        return {type:type,  value: obj};
   case amf0Types.kBooleanType:
        return {type:amf0Types.kBooleanType, value: obj};
   case amf0Types.kStringType:
        return {type:amf0Types.kStringType, value: obj};
   case amf0Types.kNullType:
        return {type:amf0Types.kNullType, value: obj};
   case amf0Types.kUndefinedType:
        return {type:amf0Types.kUndefinedType, value: obj};
   case amf0Types.kStrictArrayType:
        var result = {type:amf0Types.kStrictArrayType, value:[]};
        for(var i = 0; i < obj.length; i++){
            var value = obj[i];
            var type = AMF0Type(value);
            if(typeof value !== 'function'){
               if(type === amf0Types.kObjectType || type === amf0Types.kStrictArrayType){
                    result.value.push(AMF0Serialize(value));
               }else { //raw value
                    result.value.push({type: type, value: value});
               }
            }
        }
        return result;
   case amf0Types.kObjectType:
        var result = {type:amf0Types.kObjectType, value:[]};
        for(var key in obj){
            var value = obj[key];
            var type  = AMF0Type(value);

            if(typeof value !== 'function'){
               if(type === amf0Types.kObjectType || type === amf0Types.kStrictArrayType){
                   var e = AMF0Serialize(value);
                   e.key = key;
                   result.value.push(e);
               }else {
                    result.value.push({type: type, key: key, value: value});
               }
            }
        }
        return result;
    default:
        break;
   }
}
/* : vim: set expandtab ts=4 sts=4 sw=4 : */
