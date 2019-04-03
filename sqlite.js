module.exports = function(RED) {
    "use strict";


    var Database = function(name, config, node)
    {
        var reconnect = RED.settings.sqliteReconnectTime || 20000;
        var sqlite3 = require('sqlite3');

        this.dbname = name || config.db;
        this.mod = config.mode;

        if (config.mode === "RWC") { this.mode = sqlite3.OPEN_READWRITE | sqlite3.OPEN_CREATE; }
        if (config.mode === "RW") { this.mode = sqlite3.OPEN_READWRITE; }
        if (config.mode === "RO") { this.mode = sqlite3.OPEN_READONLY; }

        var self = this;

        this.doConnect = function(callback) {
            self.db = self.db || new sqlite3.Database(self.dbname,self.mode);
            self.db.on('open', function() {
                if (node.tick) { clearTimeout(node.tick); }
                node.log("opened "+self.dbname+" ok");
                callback(null);
            });
            self.db.on('error', function(err) {
                node.error("failed to open "+self.dbname, err);
                node.tick = setTimeout(function() { self.doConnect(); }, reconnect);
                callback(err);
            });
        }

        this.queue = [];
        this.busy = false;

        this.doQuery = function(query, params, callback, batch = false)
        {
            this.queue.push({query: query, params: params, callback: callback, batch: batch});
            this.processQueue();
        };

        this.process = function(q) {
            if (q === undefined)
            {
                this.busy = false;
                return;
            }
            var self = this;
            if (q.batch)
            {
                self.db.exec(q.query, function(err) {
                    if (err) { 
                        q.callback(err, null, self.queue.length);
                    }
                    else {
                        q.callback(null, null, self.queue.length);
                    }
                    q = self.queue.shift();
                    self.process(q);
                })
            }
            else
            {
                //node.log("Doing query " + q.query);
                self.db.all(q.query, q.params, function(err, row) {
                    if (err) { 
                        q.callback(err, null, self.queue.length)
                    }
                    else {
                        q.callback(null, row, self.queue.length)
                    }
                    q = self.queue.shift();
                    self.process(q);
                });
            }


        };

        this.processQueue = function() {
            if (this.busy) {
                return;
            }
            this.busy = true;

            if (this.queue.length === 0)
            {
                this.busy = false;
                return;
            }
            var q = this.queue.shift();
            this.process(q);
        };

        this.loadExtension = function(extension, callback) {
            self.db.loadExtension(extension, callback);
        };
    }

    function SqliteNodeDB(config) {
        RED.nodes.createNode(this,config);
        var node = this;
        node.databases = {};

        node.on('close', function (done) {
            if (node.tick) { clearTimeout(node.tick); }
            for(let key in node.databases)
            {
                if (node.databases[key].db)
                    node.databases[key].db.close(); 
            }
            done();
        });

        node.get = function(name, callback = function() {})
        {
            if (this.databases[name] === undefined || this.databases[name] === null)
            {
                this.databases[name] = new Database(name, config, node);
                var self = this;
                this.databases[name].doConnect(function(err) {
                    if (err) {
                        callback(err, null)
                        delete self.databases[name];
                    } else {
                        callback(null, self.databases[name])
                    }
                });
            }
            else
            {
                callback(null, this.databases[name]);
            }
        }
    }

    RED.nodes.registerType("sqliteQueuedDb",SqliteNodeDB);


    function SqliteNodeIn(config) {
        RED.nodes.createNode(this,config);
        this.dbName = config.dbName||"msg.database";
        this.mydb = config.mydb;
        this.db = config.db;
        this.sqlquery = config.sqlquery||"msg.topic";
        this.sql = config.sql;
        this.mydbConfig = RED.nodes.getNode(this.mydb);
        var node = this;
        node.status({});
        node.showStatus = false;

        this.database = null;


        if (node.mydbConfig) {
            node.status({fill:"green",shape:"dot",text:this.mydbConfig.mode});
           
            if (node.dbName !== "msg.database") {
                node.mydbConfig.get(node.db, function(err, db) {
                    node.log("opened database in advance");
                    node.database = db;
                    node.showStatus = true;
                });
            }
            var bind = [];


            var processQuery = function(msg, db) {
                if (node.sqlquery == "msg.topic"){
                    if (typeof msg.topic === 'string') {
                        if (msg.topic.length > 0) {
                            bind = Array.isArray(msg.payload) ? msg.payload : [];

                            db.doQuery(msg.topic, bind, function(err, row, queueSize) {
                                if (node.showStatus) {
                                    node.status({fill:queueSize == 0 ? "green" : "yellow" ,shape:"dot",text:"In queue: (" + queueSize +")"});
                                }
                                if (err) { node.error(err,msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        node.error("msg.topic : the query is not defined as a string",msg);
                        node.status({fill:"red",shape:"dot",text:"msg.topic error"});
                    }
                }
                if (node.sqlquery == "batch") {
                    if (typeof msg.topic === 'string') {
                        if (msg.topic.length > 0) {
                            db.doQuery(msg.topic, null, function(err) {
                                if (err) { node.error(err,msg);}
                                else {
                                    msg.payload = [];
                                    node.send(msg);
                                }
                            }, true);
                        }
                    }
                    else {
                        node.error("msg.topic : the query is not defined as string", msg);
                        node.status({fill:"red", shape:"dot",text:"msg.topic error"});
                    }
                }
                if (node.sqlquery == "fixed"){
                    if (typeof node.sql === 'string') {
                        if (node.sql.length > 0) {
                            db.doQuery(node.sql, bind, function(err, row) {
                                if (err) { node.error(err,msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else{
                        if (node.sql === null || node.sql == "") {
                            node.error("SQL statement config not set up",msg);
                            node.status({fill:"red",shape:"dot",text:"SQL config not set up"});
                        }
                    }
                }
                if (node.sqlquery == "prepared"){
                    if (typeof node.sql === 'string' && typeof msg.params !== "undefined" && typeof msg.params === "object") {
                        if (node.sql.length > 0) {
                            db.doQuery(node.sql, msg.params, function(err, row) {
                                if (err) { node.error(err,msg); }
                                else {
                                    msg.payload = row;
                                    node.send(msg);
                                }
                            });
                        }
                    }
                    else {
                        if (node.sql === null || node.sql == "") {
                            node.error("Prepared statement config not set up",msg);
                            node.status({fill:"red",shape:"dot",text:"Prepared statement not set up"});
                        }
                        if (typeof msg.params == "undefined") {
                            node.error("msg.params not passed");
                            node.status({fill:"red",shape:"dot",text:"msg.params not defined"});
                        }
                        else if (typeof msg.params != "object") {
                            node.error("msg.params not an object");
                            node.status({fill:"red",shape:"dot",text:"msg.params not an object"});
                        }
                    }
                }
            };
            var doQuery = function(msg) {
                if (node.dbName === "msg.database") 
                {
                    if (typeof msg.database === 'string')
                    {
                        var db = node.mydbConfig.get(msg.database, function(err, db) {
                            if (!err) {
                                if (msg.hasOwnProperty("extension")) {
                                    db.loadExtension(msg.extension, function(err) {
                                        if (err) { node.error(err,msg); }
                                        else { processQuery(msg, db); }
                                    });
                                }
                                else { processQuery(msg, db); }
                            }
                        });
                    } 
                    else 
                    {
                        node.error("msg.database : the database name is not defined as string", msg);
                        node.status({fill:"red", shape:"dot",text:"msg.database error"});
                    }
                }
                else
                {
                    if (node.database !== null)
                    {
                        if (msg.hasOwnProperty("extension")) {
                            node.database.loadExtension(msg.extension, function(err) {
                                if (err) { node.error(err,msg); }
                                else { processQuery(msg, node.database); }
                            });
                        }
                        else { processQuery(msg, node.database); }
                    }
                    else
                    {
                        node.error("Sqlite database not configured correctly");
                    }
                }

                
            }

            node.on("input", function(msg) {
                doQuery(msg);
            });
        }
        else {
            node.error("Sqlite database not configured");
        }
    }
    RED.nodes.registerType("queuedsqlite",SqliteNodeIn);
}
