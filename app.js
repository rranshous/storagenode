



// we just received an event that another node wants
// file meta data.
central.on('file_search', function(data) {

    // query the file system for that path's metadata
    filesystem.get_metadata(data.get('path'), function(metadata) {

        // send out our response if we have any
        if( metadata ) {
            central.fire('file_meta', metadata);
        };
    });
});


// we just received an event that another node
// wants file data, if we have it, send it
// the request will come with a request key, and the
// file's path. Our response should include
// a hash of the request hey and the file's hash
// so that the requesting node can ID it
central.on('file_request', function(data) {

    filesystem.get_metadata(data.get('path'), function(metadata) {

        // if we received false, there is no meta data
        if( !metadata ) { return; }

        // create the response hash
        var response_hash = hash(data.get('request_hash'), 
                                 metadata.get('file_hash'));

        // get a stream for the file's data
        var file_stream = filesystem.get_file_stream(data.get('path'));

        // forward the stream to all nodes we know about
        town.nodes.forEach(function(node) {
            node.send_file_stream(response_hash, metadata, file_stream);
        });
    });
});


// we've received an event that another node has been
// discovered in our network.
central.on('node_discovered', function(data) {

    // if we don't already know about this node, add it
    // to our set of nodes
    town.add_node(data);

});


// we received a file stream, we don't know if it's a response
// to one of our requests, or a response to someone elses.
// if it is ours, we are going to save it. if it's someone 
// else's we are going to pass it on to all the nodes we know of
central.on('file_stream_connect', 
        
    function(response_hash, metadata, data_stream) {

        // if we've already seen this response come through, ignore
        if( recent_proxies.find(response_hash) != -1 ) {
            return;
        };

        // add this response to the list we've proxied recently
        recent_proxies.append(response_hash);

        // see if we want to save this to the disk
        if( desirefactory.check_desirable(metadata) === true) {

            // get a file writer callable to save to disk
            var file_writer = filesystem.get_writer(metadata);
        
            // hook up the data stream to our file writer
            data_stream.on('data_received', file_writer.write);
        };

        // do we have any nodes we should be passing this on to?
        town.nodes.forEach(function(node) {

            // get a callable to write the file data to the node
            var node_writer = node.get_writer(metadata);

            // when the stream gets data, send it to the node
            data_stream.on('data_received', node_writer.write);
        });
    }
);


var Central = function(host, port) {

    // take note of where we are serving
    this.host = host;
    this.port = port;

    // start up a websockets server for events
    wss = new WebSocketServer({host: host, port: port, path:'/events'});

    // when a new event comes in, pass it off to our listeners
    wss.on('message', function(message) {
        var data = json_decode(message);
        this.fire(data.event, data.data);
    });

    // we also want to listen for HTTP posts of file data
    https = http.createServer(function(req, res) {

        // we only care about POSTs
        if( is_POST(req) === true ) {

            // get the response hash from the url
            // TODO
            var response_hash = undefined;

            // get the file's metadata from the headers
            var metadata = b64_json_decode(req.headers.X-FILE-METADATA)

            // create a multipart stream
            var req_stream = new multipart.Stream(req);

            // create our abstraction, to fire the parts through
            var data_stream = new EventObject();

            // let our listeners know we've got a new stream connected
            this.fire('file_stream_connected', metadata, data_stream);

            // when we get data from the stream, we want to push it
            req_stream.addListener('part', function(part) {
                part.addListener('body', function(body) {
                    data_stream.fire('data_received', body);
                })
            });
        };
    });
};
