/**
 * Module dependencies
 */

var MongoClient = require("mongodb").MongoClient;

/**
 * Manage a connection to a Mongo Server
 *
 * @param {Object} config
 * @return {Object}
 * @api private
 */

var Connection = (module.exports = function Connection(config, cb) {
  var self = this;

  // Hold the config object
  this.config = config || {};

  // Build Database connection
  this._buildConnection(function (err, db, client) {
    if (err) return cb(err);
    if (!db) return cb(new Error("no db object"));

    // Store the DB object
    self.db = db;

    // Store the client - for teardown
    self.client = client;

    // Return the connection
    cb(null, self);
  });
});

/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Create A Collection
 *
 * @param {String} name
 * @param {Object} collection
 * @param {Function} callback
 * @api public
 */

Connection.prototype.createCollection = async function createCollection(
  name,
  collection,
  cb
) {
  var self = this;

  // Create the Collection
  const result = await this.db.collection(name);
  // Create Indexes
  self._ensureIndexes(result, collection.indexes, cb);
};

/**
 * Drop A Collection
 *
 * @param {String} name
 * @param {Function} callback
 * @api public
 */

Connection.prototype.dropCollection = async function dropCollection(name, cb) {
  const success = await this.db.dropCollection(name);
  cb(success);
};

/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Build Server and Database Connection Objects
 *
 * @param {Function} callback
 * @api private
 */

Connection.prototype._buildConnection = async function _buildConnection(cb) {
  var self = this;
  
  // Build MongoDB 6.20+ compatible options (filter out undefined/deprecated)
  var connectionOptions = {
    // App identification
    appName: this.config.appname || this.config.appName,
    
    // Authentication
    authMechanism: this.config.authMechanism,
    authMechanismProperties: this.config.authMechanismProperties,
    authSource: this.config.authSource,
    
    // Compression
    compressors: this.config.compressors,
    zlibCompressionLevel: this.config.zlibCompressionLevel,
    
    // Connection timeouts
    connectTimeoutMS: this.config.connectTimeoutMS,
    socketTimeoutMS: this.config.socketTimeoutMS,
    serverSelectionTimeoutMS: this.config.serverSelectionTimeoutMS,
    heartbeatFrequencyMS: this.config.heartbeatFrequencyMS,
    
    // Connection pool
    maxPoolSize: this.config.maxPoolSize || this.config.poolSize,
    minPoolSize: this.config.minPoolSize,
    maxIdleTimeMS: this.config.maxIdleTimeMS,
    waitQueueTimeoutMS: this.config.waitQueueTimeoutMS,
    maxConnecting: this.config.maxConnecting,
    
    // Connection behavior
    directConnection: this.config.directConnection,
    loadBalanced: this.config.loadBalanced,
    
    // Read preferences
    readPreference: this.config.readPreference,
    readPreferenceTags: this.config.readPreferenceTags,
    readConcernLevel: this.config.readConcernLevel,
    maxStalenessSeconds: this.config.maxStalenessSeconds,
    localThresholdMS: this.config.localThresholdMS,
    
    // Replica set
    replicaSet: this.config.replicaSet,
    
    // Retry logic
    retryReads: this.config.retryReads !== undefined ? this.config.retryReads : true,
    retryWrites: this.config.retryWrites !== undefined ? this.config.retryWrites : true,
    
    // SRV
    srvMaxHosts: this.config.srvMaxHosts,
    srvServiceName: this.config.srvServiceName,
    
    // SSL/TLS
    ssl: this.config.ssl,
    tls: this.config.tls,
    tlsCAFile: this.config.tlsCAFile,
    tlsCertificateKeyFile: this.config.tlsCertificateKeyFile,
    tlsCertificateKeyFilePassword: this.config.tlsCertificateKeyFilePassword,
    tlsInsecure: this.config.tlsInsecure,
    
    // Write concern
    w: this.config.w,
    wTimeoutMS: this.config.wTimeoutMS || this.config.wTimeout,
    journal: this.config.journal,
    
    // Legacy SSL options (if tls not specified)
    sslCA: this.config.sslCA,
    sslCert: this.config.sslCert,
    sslKey: this.config.sslKey,
    sslValidate: this.config.sslValidate
  };
  
  // Build A Mongo Connection String
  var connectionString = "mongodb://";

  // If auth is used, append it to the connection string
  if (this.config.user && this.config.password) {
    // Ensure a database was set if auth in enabled
    if (!this.config.database) {
      throw new Error(
        "The MongoDB Adapter requires a database config option if authentication is used."
      );
    }

    connectionString += this.config.user + ":" + this.config.password + "@";
  }

  // Append the host and port
  connectionString += this.config.host + ":" + this.config.port + "/";

  if (this.config.database) {
    connectionString += this.config.database;
  }

  // Use config connection string if available
  if (this.config.url) connectionString = this.config.url;

  // Open a Connection
  const client = await MongoClient.connect(connectionString, connectionOptions);
  const database = client.db();
  cb(null, database, client);

};

/**
 * Ensure Indexes
 *
 * @param {String} collection
 * @param {Array} indexes
 * @param {Function} callback
 * @api private
 */

Connection.prototype._ensureIndexes = async function _ensureIndexes(
  collection,
  indexes,
  cb
) {
  for (const i in indexes) {
    const item = indexes[i];
    try {
      await collection.createIndex(item.index, item.options);
    } catch (err) {
      if (err.code !== 86 && err.code !== 85) {
        cb && cb(err);
      }
    }
  }
  cb && cb();
};
