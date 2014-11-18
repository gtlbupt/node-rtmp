
/**
 * Module dependencies.
 */

var amf			= require('amf');
var assert		= require('assert');
var inherits	= require('util').inherits;
var events		= require('events');
var Parser		= require('stream-parser');
var debug		= require('debug')('flv:decoder');

// node v0.8.x compat
//if (!Writable) Writable = require('readable-stream/writable');

/**
 * Module exports.
 */

module.exports = Encoder;


/**
 * The `Encoder` class is a `Writable` stream that expects an FLV file to be
 * written to it, and it will output the embedded audio and video stream in
 * "audio" and "video" events (respectively).
 *
 * A "metadata" event gets emitted every time a metadata chunk is encountered
 * inside the FLV file.
 *
 * Reference:
 *  - http://osflash.org/flv#flv_format
 *  - http://en.wikipedia.org/wiki/Flash_Video
 *
 * @param {Object} opts optional "options" object
 * @api public
 */

function writeUInt24BE (buffer, val, offset){
	buffer[offset + 0] = (val >> 16) & 0xff; 
	buffer[offset + 1] = (val >> 8) & 0xff; 
	buffer[offset + 2] = val & 0xff; 
	return buffer;
};

function Encoder (opts) {
	this.defaultChunkSize = opts ? (opts.chunkSize || 128) :  128;
}

inherits(Encoder, require('events').EventEmitter);

Encoder.prototype.headerSize = [11,7,3,0];
Encoder.prototype.defaultChunkSize = 128;

/**
*/
Encoder.prototype.makeChunkBasicHeader  = function(obj){
	var fmt = obj.fmt;
	var csid = obj.csid;
	if(csid < 64){//[2,63]
		var h = new Buffer(1);    
		var v = (fmt<<6)|csid;
		h.writeUInt8(v,0);
		return h;
	}else if(csid < 320){ //[64+0,64+255] => [64,319]
		var h = new Buffer(2);    
		var v = 0;
		v = (fmt<<6)|0;
		h.writeUInt8(v,0);
		h.writeUInt8(csid-64,1);
		return h;
	}else if(csid < 65600){
		var h = new Buffer(3);    
		var v = 0;
		v = (fmt<<6)|0;
		h.writeUInt8(v,0);
		h.writeUInt16LE(csid-64,1);
		return h;
	}else {
		throw new Error("Bad csid");    
	}
};

/**
 * obj{
 *	fmt:
 *	timestamp:, 
 *	timestampDelta:,
 *	msgType:,
 *	msgLength:,
 *	streamId:
 * }
 */
Encoder.prototype.makeChunkMessageHeader = function(obj){
	var h = null;
	var offset = 0;
	var hasExtendTs = false;
	if(obj.absTimestamp >= 0x00ffffff){
		hasExtendTs = true;
	}
	switch(obj.fmt){
		case 0:
			h = new Buffer(11);
			offset = 0;
			if(hasExtendTs){
				writeUInt24BE(h, 0xffffff, offset); offset += 3;
			} else {
				writeUInt24BE(h, obj.absTimestamp, offset); offset += 3;
			}
			writeUInt24BE(h, obj.msgLength, offset); offset += 3;
			h.writeUInt8(obj.msgType,offset); offset += 1;
			h.writeUInt32LE(obj.streamId, offset); offset += 4;
			break;
		case 1:
			h = new Buffer(7);
			offset = 0;
			writeUInt24BE(h, obj.timestamp, offset); offset += 3;
			writeUInt24BE(h, obj.msgLength, offset); offset += 3;
			h.writeUInt8(obj.msgType,offset); offset += 1;
			break;
		case 2:
			h = new Buffer(3);
			offset = 0;
			writeUInt24BE(h,obj.timestamp,offset); offset += 3;
			break;
		case 3:
			h = new Buffer(0);
			break; 
	}
	if( hasExtendTs && (obj.fmt == 0 || obj.fmt == 3)){
		var ts = new Buffer(4);
		ts.writeUInt32BE(obj.absTimestamp, 0);
		h = Buffer.concat([h, ts]);  
	}
	return h;
}

Encoder.prototype.makeRTMPHeader = function(obj){
	return  Buffer.concat(
			[
			this.makeChunkBasicHeader(obj),
			this.makeChunkMessageHeader(obj)
			]
			);
};

Encoder.prototype.makeRTMPBody = function(obj){
	var body  = obj.rawData;
	if(body.length === 0){
		return new Buffer(0); 
	}
	var buff = new Buffer(body.length + Math.floor((body.length+this.defaultChunkSize-1)/this.defaultChunkSize) - 1);
	var offset = 0;

	var first = true;
	var type3header = new Buffer([(3<<6)|obj.csid]);
	do {
		if(first){
			first = false; 
		}else{
			type3header.copy(buff,offset);
			offset += 1;
		}
		var len = Math.min(body.length, this.defaultChunkSize);
		var chunk = body.slice(0, len);
		chunk.copy(buff, offset);
		offset += len;

		body = body.slice(len);
	} while(body.length > 0);

	return buff;
};

//(1)
Encoder.prototype.setChunkSize = function(size){
	var msg = {
		fmt:    0,
		csid:   2,
		timestamp:  0,
		timestampDelta: 0,   
		msgLength:  4,
		msgType :   1, // set chunk size
		streamId:   0
	};
	var header = this.makeRTMPHeader(msg);
	var body   = new Buffer(4);
	body.writeUInt32BE(size, 0);
	this.defaultChunkSize = size;
	return Buffer.concat([header, body]);
}

//(2)
Encoder.prototype.AbortMessage = function(obj){
}

//(3)
Encoder.prototype.Acknowledgement = function(size){
	var msg = {
		fmt:    0,
		csid:   2,
		timestamp:  0,
		timestampDelta: 0,   
		msgLength:  4,
		msgType :   3, // set chunk size
		streamId:   0
	};
	var header = this.makeRTMPHeader(msg);
	var body   = new Buffer(4);
	body.writeUInt32BE(size);
	return buffer.concat(header, body);
}

//(4)
Encoder.prototype.UserControlMessageEvents = function(event, data){
	var header = null;
	var body = null;
	switch(event){
		case 0: //Stream Begin    
			body = new Buffer(2+4);
			body.writeUInt16BE(event, 0);
			body.writeUInt32BE(data,  2);
			break; 
		case 1://Stream EOF
			body = new Buffer(2+4);
			body.writeUInt16BE(event, 0);
			body.writeUInt32BE(data, 0);
			break;
		case 2://Stream Dry
			/**
			 * 2-byte		| 4-byte
			 * eventType	| streamID	
			 * bufferLength: buffer size, in milliseconds.
			 */
			body = new Buffer(2+4);
			body.writeUInt16BE(event, 0);
			body.writeUInt32BE(data, 0);
			break;
		case 3://SetBufferLength
			/**
			 * 2-byte		| 4-byte	| 4-byte
			 * eventType	| streamID	| bufferLength
			 * bufferLength: buffer size, in milliseconds.
			 */
			assert(arguments.length >= 3);
			body = new Buffer(2+4+4);
			body.writeUInt16BE(event, 0);
			var streamId = arguments[1];
			body.writeUInt32BE(streamId, 2);
			var bufferLength = arguments[2];
			body.writeUInt32BE(bufferLength, 6);
			break;
		case 4://StreamIsRecorded
			/**
			 * 2-byte		| 4-byte 
			 * EventType	| streamID 
			 */
			assert(arguments.length >= 2);
			body = new Buffer(2+4);
			body.writeUInt16BE(event, 0);
			body.writeUInt32BE(data, 2);
			break;
		case 6://PingRequest
			/**
			 * 2-byte		| 4-byte 
			 * EventType	| timestamp 
			 * timestamp: server's local timestamp
			 */
			assert(arguments.length >= 2);
			body = new Buffer(2+4);
			body.writeUInt16BE(event, 0);
			body.writeUInt32BE(data, 2);
			break;
		case 7://PingResponse
			/**
			 * 2-byte		| 4-byte 
			 * EventType	| timestamp 
			 * timestamp: recv from PingRequest.
			 */
			assert(arguments.length >= 2);
			body = new Buffer(2+4);
			body.writeUInt16BE(event, 0);
			body.writeUInt32BE(data, 2);
			break;
		default:
			throw new Error("Unknown UserControlMessageEvent %", event);
			break;
	}
	var msg = {
		fmt:    0,
		csid:   2,
		timestamp:  0,
		timestampDelta: 0,   
		msgLength:  body.length,
		msgType :   4, 
		streamId:   0
	};
	header = this.makeRTMPHeader(msg);
	return Buffer.concat([header, body]);
}

Encoder.prototype.WindowAcknowledgementSize = function(size){
	var msg = {
		fmt:    0,
		csid:   2,
		timestamp:  0,
		timestampDelta: 0,   
		msgLength:  4,
		msgType :   5, 
		streamId:   0
	};
	var header = this.makeRTMPHeader(msg);
	var body   = new Buffer(header.length + 4);
	header.copy(body,0);
	body.writeUInt32BE(size, header.length);
	return body;
}


Encoder.prototype.SetPeerBandwidth = function(AckWindowSize, limitType){
	var msg = {
		fmt:    0,
		csid:   2,
		timestamp:  0,
		timestampDelta: 0,   
		msgLength:  5,
		msgType :   6, 
		streamId:   0
	};
	var header = this.makeRTMPHeader(msg);
	var body   = new Buffer(header.length + 5);
	header.copy(body,0);
	body.writeUInt32BE(AckWindowSize, header.length + 0);
	body.writeUInt8(limitType, header.length + 4);
	return body;
}

Encoder.prototype.AVMessage = function(obj){
	var bufs = [];
	var opts = {
		fmt:    obj.fmt === 1 ? 1:0,
		csid:   obj.csid || 4,
		timestamp:  obj.timestamp,
		timestampDelta: 0,   
		msgLength:  obj.msgLength,
		msgType :   obj.msgType, 
		streamId:   obj.streamId
	};
	var header = this.makeRTMPHeader(opts);
	// AudioMessage = header + body
	// body =  chunkHeader + chunks(obj.msgLength);
	// 
	var body  = obj.rawData;
	var bodyLength = body.length;
	if(bodyLength === 0){
		return null; 
	}
	if(bodyLength > this.defaultChunkSize){
		bufs.push(header);
		bufs.push(body.slice(0, this.defaultChunkSize)); 

		body = body.slice(this.defaultChunkSize);
		while(true){
			var type3header = new Buffer([(3<<6)|opts.csid]);
			var bodyChunk   = body.slice(0, this.defaultChunkSize); 

			bufs.push(type3header);
			bufs.push(bodyChunk);

			body = body.slice(bodyChunk.length);
			if(bodyChunk.length <= this.defaultChunkSize){
				break; 
			}
		}
	}else { // bodyLength <= this.defaultChunkSize
		bufs.push(header);
		bufs.push(body);  
	}
	return Buffer.concat(bufs);

	var len = header.length + Math.floor((obj.msgLength+this.defaultChunkSize-1)/this.defaultChunkSize) - 1 +       obj.msgLength;

	var body   = new Buffer(len);

	var offset = 0;


	var msg = obj.rawData;
	for(var i=0,e = this.defaultChunkSize; i < msg.length; i+=
			this.defaultChunkSize, e += this.defaultChunkSize){
				if(e >= msg.length){
					e = msg.length; 
				}

				header.copy(body,offset, 0,header.length);
				offset += header.length;

				msg.copy(body,offset,i,e);
				offset += e-i;

				if(i === 0){
					header = new Buffer([ (3<<6)|opts.csid ]);
				}
			}
	return body;
}

Encoder.prototype.MetadataMessage = function(obj){
	return this.AVMessage(obj);
};

Encoder.prototype.AudioMessage = function(obj){
	return this.AVMessage(obj);
};

Encoder.prototype.VideoMessage = function(obj){
	return this.AVMessage(obj);
};

Encoder.prototype.AMF0CmdMessage = function(obj){
	var msg = {
		fmt:    obj.fmt != null ? obj.fmt : 1,
		csid:   obj.csid || 0x03,
		timestamp:  0,
		timestampDelta: 0,   
		msgLength:  obj.msgLength,
		msgType :   0x14, 
		streamId:   obj.streamId || 0
	};
	var header = this.makeRTMPHeader(msg);
	//console.log('AMF0CmdMessage RTMP Header length %d, header', header.length, header);

	var body   = new Buffer(
			header.length -1 + Math.floor((obj.msgLength +
					this.defaultChunkSize-1)/this.defaultChunkSize)
			+ obj.msgLength);

	var offset = 0;

	var defaultHeader = new Buffer([0xc3]);
	for(var i=0 ; i < obj.msg.length; i += this.defaultChunkSize){
		var e = i + this.defaultChunkSize;
		if (e > obj.msg.length){
			e = obj.msg.length;   
		}

		//console.log('offset: %d', offset);
		header.copy(body, offset, 0, header.length);
		offset += header.length;
		//console.log('offset: %d', offset);
		obj.msg.copy(body, offset, i,e);
		offset += e-i;
		//console.log('offset: %d', offset);


		header = defaultHeader;
	}
	return body;
}


Encoder.prototype.writeMsg = function(info, header, body){
	switch(info.msgType){
		case 0x01:
			// HandleChangeChunkSize
			this.SetChunkSize();
			break;     
		case 0x02:
			//RTMP abort
			this.AbortMessage();
			break; 
		case 0x03:
			/* bytes read report*/
			this.Acknowledgement();
			break;
		case 0x04:
			/* ctrl */
			//HnadleCtrl*(;
			this.UserControlMessages();
			break;
		case 0x05:
			/* server bw */
			//HandleServerBW
			this.WindowAcknowledgementSize()
				break;
		case 0x06:
			/* client BW */
			//HandleClientBW
			this.SetPeerBandwidth();
			break;
		case 0x07:
			/* Edge */
			//HandleEdge
		case 0x08:
			// Audio Data
			//HandleAudio();
			this.AudioMessage();
			break;
		case 0x09:
			// Video Data
			// HandleVideo
			this.VideoMessage();
		case 0x0F:
			/* Flex stream send */
			this.AMF3DataMessage();
			break;
		case 0x10:
			// Flex shared object
			this.AMF3SharedObjectMessage();
			break;
		case 0x11:
			// Flex Message
			this.AMF3CmdMessage();
			break;
		case 0x12:
			//HandleMetadata();
			this.AMF0DataMessage();
			break;
		case 0x13:
			// share object
			this.AMF0SharedObjectMessage();
			break;
		case 0x14:
			/* HandleIvoke*/
			this.AMF0CmdMessage();
		case 0x15:
			break;
		case 0x16:
			this.AggregateMessage();
			break;
		default:
			break;
	}

	// writeHeader
	var defaultHeader = new Buffer([0x03]);
	for(var i=0,e = this.defaultChunkSize; i < body.length; i+=
			this.defaultChunkSize, e += this.defaultChunkSize){
				if(e >= body.length){
					e = body.length; 
				}
				this.socket.write(head);  
				this.socket.write(body.slice(i,e));
				head = defaultHeader;
			}
};

Encoder.prototype._onc0 = function(buf){
	var c0 = buf.readUInt8(0);   
	assert(c0 === 0x03);
	if(c0 !== 0x03){
		return this.emit('error', new Error('expected RTMP version : 0x03, got: ' + c0));      
	}
	this.c0 = c0;
	this._bytes(1536, this._onc1);
	this.emit('c0', buf);
};

Encoder.prototype._onc1 = function(buf){
	var c1  = buf.slice(0);
	this.c1 = c1;
	this._bytes(1536, this._onc2);
	this.emit('c1', buf);
};
Encoder.prototype._onc2 = function(buf){
	var c2 = buf.slice(0);
	this.c2 = c2;
	this._bytes(1, this._onBasicHdr);       
	this.emit('c2', buf);
	this.chunks = [];
	this.chunkReadSize = 0;
};

Encoder.prototype._onBasicHdr = function(buf){
	var b    = buf.readUInt8(0);
	var fmt  = b >> 6;
	var csid = b & 0x3f;
	this.fmt = fmt;
	this.csid = csid;
	var arg = {
		'fmt': fmt,
		'csid': csid 
	};
	this.emit('BasicHeader', arg);
	var size = this.headerSize[fmt]; 

	if(csid === 0 || csid === 1){
		this._bytes(csid+1, this._onExtraChunkBasicHdr);       
	}else if(size > 0){
		this._bytes(size, this._onMessageHdr);       
	}else {
		this._onMessageHdr.call(this,new Buffer(0));
	}

	this.chunks.push(buf);
};

Encoder.prototype._onExtraChunkBasicHdr = function(buf){
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

	if(size > 0){
		this._bytes(size, this._onMessageHdr);       
	}else {
		this._onMessageHdr.call(this,new Buffer(0));
	}

	this.chunks.push(buf);
}
Encoder.prototype._onMessageHdr = function(buf){
	switch(this.fmt){
		case 0:
			assert(buf.length == 11);
			var timestamp = readUInt24BE(buf,0);
			var msgLength = readUInt24BE(buf,3);
			var msgType   = buf.readUInt8(6);
			var sid       = buf.readUInt32BE(7);

			this.timestamp = timestamp;
			this.msgLength = msgLength;
			this.msgType   = msgType;
			this.streamId  = sid;
			this.msg       = new Buffer(msgLength);
			break;
		case 1:
			assert(buf.length == 7);
			var timestamp = readUInt24BE(buf,0);
			var msgLength = readUInt24BE(buf,3);
			var msgType   = buf.readUInt8(buf,6);
			break;
		case 2:
			assert(buf.length == 3);
			var timestamp = readUInt24BE(buf,0);
			break;
		case 3:
			break;     
	}
	this.chunks.push(buf);

	if('time' in this && this.time === 0x00ffffff){
		this._bytes(4, this._onExtendTimestamp);       
	}else {
		var toReadSize = this.msgLength - this.chunkReadSize;
		if( toReadSize < this.defaultChunkSize){
			this._bytes(toReadSize, this._onChunkData);       
		}else{
			this._bytes(this.defaultChunkSize, this._onChunkData);       
		}
	}
	this.emit('MessageHdr',{fmt:this.fmt,msgLength: this.msgLength});
};

Encoder.prototype._onExtrendTimestamp = function(buf){
	var time = buf.readUInt32BE(buf);  
	this.timestamp = time;
	var toReadSize = this.msgLength - this.chunkReadSize;
	if( toReadSize < this.defaultChunkSize){
		this._bytes(toReadSize, this._onChunkData);       
	}else{
		this._bytes(this.defaultChunkSize, this._onChunkData);       
	}
}
Encoder.prototype._onChunkData = function(buf){
	this.chunks.push(buf);

	buf.copy(this.msg,this.chunkReadSize);
	this.chunkReadSize += buf.length;

	if(this.chunkReadSize < this.msgLength){
		this._bytes(1, this._onBasicHdr);       
	}else { //already read a message
		assert(this.chunkReadSize === this.msgLength);

		/*
		   var position = {offset: 0};             
		   var args = [];
		   while(position.offset < this.msg.length){
		   var ext = amf.read(this.msg, position);
		   args.push(ext);
		   }
		   */
		this.onMessage(Buffer.concat(this.chunks)); //call onMessage to handle this
		this.chunks.splice(0);
	}
	//this._bytes(1, this._onBasicHdr);
};

//(1)
Encoder.prototype.SetChunkSize = function () {
	var chunkSize = this.msg.readUInt32BE(0); 
};
//(2)
Encoder.prototype.AbortMessage = function () {};
//(3)
Encoder.prototype.Acknowledgement = function () {};
//(4)
Encoder.prototype.UserControlMessages = function () {};
//(5)
//Encoder.prototype.WindowAcknowledgementSize = function () {};
//(6)
//Encoder.prototype.SetPeerBandwidth = function () {};
//(8)
//Encoder.prototype.AudioMessage = function () {};
//(9)
//Encoder.prototype.VideoMessage = function () {}; 
//(15)
Encoder.prototype.AMF3DataMessage= function () {};
//(16)
Encoder.prototype.AMF3SharedObjectMessage = function () {};
//(17)
Encoder.prototype.AMF3CmdMessage = function () {};
//(18)
Encoder.prototype.AMF0DataMessage = function(obj){
	//console.log('AMF0DataMessage: ', obj);
	var msg = {
		fmt:    0,
		csid:   obj.csid || 0x05,
		timestamp:  0,
		timestampDelta: 0,  
		msgLength:  obj.msgLength,
		msgType :   0x12, 
		streamId:   obj.streamId || 0
	};
	var header = this.makeRTMPHeader(msg);
	//console.log('AMF0DataMessage RTMP Header length %d, header', header.length, header);

	var body   = new Buffer(
			header.length -1 + Math.floor((obj.msgLength +
					this.defaultChunkSize-1)/this.defaultChunkSize)
			+ obj.msgLength);

	var offset = 0;

	var defaultHeader = new Buffer([0xc3]);
	for(var i=0 ; i < obj.msg.length; i += this.defaultChunkSize){
		var e = i + this.defaultChunkSize;
		if (e > obj.msg.length){
			e = obj.msg.length;   
		}

		header.copy(body, offset, 0, header.length);
		offset += header.length;
		obj.msg.copy(body, offset, i,e);
		offset += e-i;


		header = defaultHeader;
	}
	return body;
};
//(19)
Encoder.prototype.AMF0SharedObjectMessage = function () {};
//(20)
/*
   Encoder.prototype.AMF0CmdMessage = function () {
   try{
   var position = {offset: 0};             
   var args = [];
   while(position.offset < this.msg.length){
   var ext = amf.read(this.msg, position);
   args.push(ext);
   }
   console.log('Function[HandleInvoke], argument', args);
   this.emit('connect', args, Buffer.concat(this.chunks) );
   }catch(e){

   } 
   };
   */
//(22)
Encoder.prototype.AggregateMessage = function () {
};

Encoder.prototype.onMessage = function (obj, rawData) {
	console.log("msgType", this.msgType);
	switch(this.msgType){
		case 0x01:
			// HandleChangeChunkSize
			this.SetChunkSize();
			break;     
		case 0x02:
			//RTMP abort
			this.AbortMessage();
			break; 
		case 0x03:
			/* bytes read report*/
			this.Acknowledgement();
			break;
		case 0x04:
			/* ctrl */
			//HnadleCtrl*(;
			this.UserControlMessages();
			break;
		case 0x05:
			/* server bw */
			//HandleServerBW
			this.WindowAcknowledgementSize()
				break;
		case 0x06:
			/* client BW */
			//HandleClientBW
			this.SetPeerBandwidth();
			break;
		case 0x07:
			/* Edge */
			//HandleEdge
		case 0x08:
			// Audio Data
			//HandleAudio();
			this.AudioMessage();
			break;
		case 0x09:
			// Video Data
			// HandleVideo
			this.VideoMessage();
		case 0x0F:
			/* Flex stream send */
			this.AMF3DataMessage();
			break;
		case 0x10:
			// Flex shared object
			this.AMF3SharedObjectMessage();
			break;
		case 0x11:
			// Flex Message
			this.AMF3CmdMessage();
			break;
		case 0x12:
			//HandleMetadata();
			this.AMF0DataMessage();
			break;
		case 0x13:
			// share object
			this.AMF0SharedObjectMessage();
			break;
		case 0x14:
			/* HandleIvoke*/
			this.AMF0CmdMessage();
		case 0x15:
			break;
		case 0x16:
			this.AggregateMessage();
			break;
		default:
			break;
	}
}

Encoder.prototype._onsignature = function (buf) {
	var sig = buf.toString('ascii');
	debug('onsignature(%j)', sig);
	if ('FLV' != sig) {
		return this.emit('error', new Error('invalid FLV signature: ' + JSON.stringify(sig)));
	}
	this.signature = sig;
	this._bytes(1, this._onversion);
};

Encoder.prototype._onversion = function (buf) {
	var ver = buf.readUInt8(0);
	debug('onversion(%d)', ver);
	if (1 !== ver) {
		// currently 1 is the only version for known FLV files
		return this.emit('error', new Error('expected flv version 1, got: ' + ver));
	}
	this.version = ver;
	this._bytes(1, this._onflags);
};

Encoder.prototype._onflags = function (buf) {
	var flags = buf.readUInt8(0);
	debug('onflags(%d)', flags);
	this.flags = flags;
	this._bytes(4, this._onoffset);
};

Encoder.prototype._onoffset = function (buf) {
	var offset = buf.readUInt32BE(0);
	debug('onoffset(%d)', offset);
	// assert offset === 9
	this.offset = offset;
	this._bytes(4, this._onprevioustagsize);
};

Encoder.prototype._onprevioustagsize = function (buf) {
	var size = buf.readUInt32BE(0);
	debug('onprevioustagsize(%d)', size);
	// assert size === 0
	this._bytes(1, this._ontagtype);
};

Encoder.prototype._ontagtype = function (buf) {
	var type = buf.readUInt8(0);
	debug('ontagtype(%d)', type);
	this.currentTag = { type: type };
	this._bytes(3, this._ontagbodylength);
};

Encoder.prototype._ontagbodylength = function (buf) {
	var length = readUInt24BE(buf, 0);
	debug('ontagbodylength(%d)', length);
	this.currentTag.bodyLength = length;
	//this._bytes(3, this._ontagtimestamp);
	this._bytes(4, this._ontagtimestamp);
};

Encoder.prototype._ontagtimestamp = function (buf) {
	//var time = readUInt24BE(buf, 0);
	var time = buf.readUInt32BE(0);
	debug('ontagtimestamp(%d)', time);
	this.currentTag.timestamp = time;
	this._bytes(3, this._ontagstreamid);
};

Encoder.prototype._ontagstreamid = function (buf) {
	var id = readUInt24BE(buf, 0);
	debug('ontagstreamid(%d)', id);
	this.currentTag.id = id;
	var len = this.currentTag.bodyLength;
	if (0 == len) {
		// this shouldn't really happen, but _bytes() throws an assertion error
		// if 0 is passed in, so just skip to the next step if 0 is reported
		this._bytes(4, this._onprevioustagsize);
	} else {
		this._bytes(len, this._ontagbody);
	}
};

Encoder.prototype._ontagbody = function (buf, fn) {
	debug('ontagbody(%d bytes)', buf.length);

	// queue the next step before we start any async stuff
	this._bytes(4, this._onprevioustagsize);

	var stream;
	this.currentTag.body = buf;
	switch (this.currentTag.type) {
		case 0x08: // audio
			debug('got "audio" tag');
			stream = this._stream();
			var meta = buf.readUInt8(0);
			var soundType = (meta & 0x01) >> 0; // 0: mono, 1: stereo
			var soundSize = (meta & 0x02) >> 1; // 0: 8-bit, 1: 16-bit
			var soundRate = (meta & 0x0C) >> 2; // 0: 5.5 kHz (or speex 16kHz), 1: 11 kHz, 2: 22 kHz, 3: 44 kHz
			var soundFormat = (meta & 0xf0) >> 4; // 0: Uncompressed, 1: ADPCM, 2: MP3, 5: Nellymoser 8kHz mono, 6: Nellymoser, 10: AAC, 11: Speex, more
			this.currentTag.soundType = soundType;
			this.currentTag.soundSize = soundSize;
			this.currentTag.soundRate = soundRate;
			this.currentTag.soundFormat = soundFormat;
			//console.error(this.currentTag);

			if (soundFormat == 10) {
				// AAC audio needs special handling
				var aacType = buf.readUInt8(1);
				var bits;
				if (0 == aacType) {
					// AAC sequence header
					// This is an AudioSpecificConfig as specified in ISO 14496-3
					var header = buf.slice(2);
					assert(header.length >= 2);

					bits = ((header[0] & 0xff) * 256 + (header[1] & 0xff)) << 16;
					stream.aacProfile = readBits(bits, 5) - 1;
					bits <<= 5;
					stream.sampleRateIndex = readBits(bits, 4);
					bits <<= 4;
					stream.channelConfig = readBits(bits, 4);

					fn();
				} else {
					// AAC raw (no ADTS header)
					var audioData = buf.slice(2);
					var dataSize = audioData.length;

					// need to construct an ADTS header manually...
					// see http://wiki.multimedia.cx/index.php?title=ADTS for format spec
					// https://github.com/gangverk/flvdemux/blob/master/src/com/gangverk/FLVDemuxingInputStream.java
					// http://codeartisan.tumblr.com/post/11943952404/playing-flv-wrapped-aac-streams-from-android
					var adts = new Buffer(7);
					bits = 0;
					bits = writeBits(bits, 12, 0xFFF);
					bits = writeBits(bits, 3, 0);
					bits = writeBits(bits, 1, 1);
					adts[0] = (bits >> 8);
					adts[1] = (bits);

					bits = 0;
					bits = writeBits(bits, 2, stream.aacProfile);
					bits = writeBits(bits, 4, stream.sampleRateIndex);
					bits = writeBits(bits, 1, 0);
					bits = writeBits(bits, 3, stream.channelConfig);
					bits = writeBits(bits, 4, 0);
					bits = writeBits(bits, 2, (dataSize + 7) & 0x1800);

					adts[2] = (bits >> 8);
					adts[3] = (bits);

					bits = 0;
					bits = writeBits(bits, 11, (dataSize + 7) & 0x7FF);
					bits = writeBits(bits, 11, 0x7FF);
					bits = writeBits(bits, 2, 0);
					adts[4] = (bits >> 16);
					adts[5] = (bits >> 8);
					adts[6] = (bits);

					// first write the ADTS header
					stream._pushAndWait(adts, function () {
						// then write the raw AAC data
						stream._pushAndWait(audioData, fn);
					});

					// alternate way using `Buffer.concat()` instead - benchmark someday
					/*var b = Buffer.concat([ adts, audioData ]);
					  stream._pushAndWait(b, fn);*/
				}
			} else {
				// the raw audio data Buffer (MP3 data or whatever...)
				this.currentTag.audioData = buf.slice(1);
				stream._pushAndWait(this.currentTag.audioData, fn);
			}
			break;
		case 0x09: // video
			debug('got "video" tag');
			// TODO: implement
			stream = this._stream();
			fn();
			break;
		case 0x12: // metadata
			debug('got "metadata" tag');
			// metadata is in AMF format, 2 packets
			var position = { offset: 0 };

			// first packet is an AMF "string", the event name
			var name = amf.read(buf, position);
			this.currentTag.name = name;

			// second packet is the "data" payload, which is an AMF "array"
			var data = amf.read(buf, position);
			this.currentTag.data = data;

			this.emit('metadata', name, data, this.currentTag);
			fn();
			break;
		default:
			this.emit('error', new Error('unknown tag type: ' + this.currentTag.type));
			return;
	}
};

/**
 * Returns a `EncoderStream` instance that corresponds with the current "tag"
 * being parsed.
 *
 * @return {EncoderStream} The EncoderStream instance for the current "tag"
 * @api private
 */

Encoder.prototype._stream = function () {
	var name = this.currentTag.type + '-' + this.currentTag.id;
	var stream = this[name];
	var type = this.currentTag.type;
	if (!stream) {
		debug('creating EncoderStream instance for %j', name);
		stream = this[name] = new EncoderStream();

		// also add them to an array so that we can iterate the streams in "finish"
		this._streams.push(stream);

		// emit an "audio" or "video" event
		if (0x08 == type) { // audio
			name = 'audio';
		} else if (0x09 == type) { // video
			name = 'video';
		} else {
			throw new Error('unsupported "stream" type: ' + type);
		}
		this.emit(name, stream);
	}
	return stream;
};

/**
 * Called for the Encoder's "finish" event. Pushes the `null` packet to the audio
 * and/or video stream in the FLV file, so that they emit "end".
 *
 * @api private
 */

Encoder.prototype._onfinish = function () {
	debug('"finish" event');
	this._streams.forEach(function (stream) {
		stream._pushAndWait(null, function () {
			debug('`null` packet flushed');
		});
	});
	this._streams.splice(0); // empty
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

function readBits (x, length) {
	return (x >> (32 - length));
}

function writeBits (x, length, value) {
	var mask = 0xffffffff >> (32 - length);
	x = (x << length) | (value & mask);
	return x;
}
