/**
 * Module dependencies
 */

var _ = require("@sailshq/lodash");
var Errors = require("waterline-errors").adapter;
var utils = require("./utils");
var Document = require("./document");
var Query = require("./query");

/**
 * Manage A Collection
 *
 * @param {Object} definition
 * @api public
 */

var Collection = (module.exports = function Collection(definition, connection) {
  // Set an identity for this collection
  this.identity = "";

  // Hold Schema Information
  this.schema = null;

  // Hold a reference to an active connection
  this.connection = connection;

  // Hold the config object
  var connectionConfig = connection.config || {};
  this.config = _.extend({}, connectionConfig.wlNext);

  // Hold Indexes
  this.indexes = [];

  // Parse the definition into collection attributes
  this._parseDefinition(definition);

  // Build an indexes dictionary
  this._buildIndexes();

  return this;
});

/////////////////////////////////////////////////////////////////////////////////
// PUBLIC METHODS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Find Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.find = async function find(criteria, cb) {
  var self = this,
    query;

  // Catch errors from building query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch (err) {
    return cb(err);
  }
  var collection = this.connection.db.collection(self.identity);

  // Check for aggregate query
  if (query.aggregate) {
    const aggregate = [
      { $match: query.criteria.where || {} },
      { $group: query.aggregateGroup },
    ];

    try {
      const results = await collection.aggregate(aggregate).toArray();
      var mapped = results.map(function (result) {
        for (var key in result._id) {
          result[key] = result._id[key];
        }
        delete result._id;
        return result;
      });
      cb(null, mapped);
    } catch (err) {
      cb(err, mapped);
    }
  } else {
    var where = query.criteria.where || {};
    var queryOptions = _.omit(query.criteria, "where");
    // var limit = query.criteria.limit;
    // Run Normal Query on collection
    try {
      // const docs = limit
      //   ? await collection
      //       .find(where, query.select, queryOptions)
      //       .limit(limit)
      //       .toArray()
      //   : await collection.find(where, query.select, queryOptions).toArray();
      const docs = await collection
        .find(where, queryOptions)
        .project(query.select)
        .toArray();
      cb(null, utils.normalizeResults(docs, self.schema));
    } catch (err) {
      if (err) return cb(err);
    }
  }
};

/**
 * Stream Documents
 *
 * @param {Object} criteria
 * @param {Object} stream
 * @api public
 */
Collection.prototype.stream = function find(criteria, stream) {
  var self = this,
    query;

  // Ignore `select` from waterline core
  if (typeof criteria === "object") {
    delete criteria.select;
  }

  // Catch errors from building query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch (err) {
    return stream.end(err); // End stream
  }

  var collection = this.connection.db.collection(self.identity);

  var where = query.criteria.where || {};
  var queryOptions = _.omit(query.criteria, "where");

  // Run Normal Query on collection
  var dbStream = collection.find(where, queryOptions).stream();

  // For each data item
  dbStream.on("data", function (item) {
    // Pause stream
    dbStream.pause();

    var obj = utils.rewriteIds([item], self.schema)[0];

    stream.write(obj, function () {
      dbStream.resume();
    });
  });

  // Handle error, an 'end' event will be emitted after this as well
  dbStream.on("error", function (err) {
    stream.end(err); // End stream
  });

  // all rows have been received
  dbStream.on("close", function () {
    stream.end();
  });
  // stream has ended
  dbStream.on("end", function () {
    stream.end();
  });
};

/**
 * Insert A New Document
 *
 * @param {Object|Array} values
 * @param {Function} callback
 * @api public
 */

Collection.prototype.insert = async function insert(values, cb) {
  var self = this;
  // Normalize values to an array
  if (!Array.isArray(values)) values = [values];

  // Build a Document and add the values to a new array
  var docs = values.map(function (value) {
    return new Document(value, self.schema).values;
  });

  try {
    const insertResults = await this.connection.db
      .collection(this.identity)
      .insertMany(docs);
    const results = await this.connection.db
      .collection(this.identity)
      .find(
        { _id: { $in: Object.values(insertResults.insertedIds) } },
        { _id: 1 }
      )
      .toArray();
    cb(null, utils.rewriteIds(results, self.schema));
  } catch (err) {
    if (err) return cb(err);
  }
};

/**
 * Update Documents
 *
 * @param {Object} criteria
 * @param {Object} values
 * @param {Function} callback
 * @api public
 */

Collection.prototype.update = async function update(criteria, values, cb) {
  var self = this,
    query;

  // Ignore `select` from waterline core
  if (typeof criteria === "object") {
    delete criteria.select;
  }

  // Catch errors build query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch (err) {
    return cb(err);
  }

  values = new Document(values, this.schema).values;

  // Mongo doesn't allow ID's to be updated
  if (values.id) delete values.id;
  if (values._id) delete values._id;

  // Lookup records being updated and grab their ID's
  // Useful for later looking up the record after an insert
  // Required because options may not contain an ID
  try {
    const collection = this.connection.db.collection(self.identity);
    const records = await collection
      .find(query.criteria.where)
      .project({ _id: 1 })
      .toArray();
    // if (records.length < 1) return cb(Errors.NotFound);
    if (!records) return cb(Errors.NotFound); // is this really right?
    // Build an array of records id
    const updatedRecords = records.map((record) => record._id);

    // Update the records
    const update = await collection.updateMany(query.criteria.where, {
      $set: values,
    });
    if (update.acknowledged) {
      // Look up newly inserted records to return the results of the update
      const results = await collection
        .find({ _id: { $in: updatedRecords } })
        .toArray();
      cb(null, utils.rewriteIds(results, self.schema));
    }
  } catch (err) {
    if (err) return cb(err);
  }
};

/**
 * Destroy Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.destroy = async function destroy(criteria, cb) {
  const self = this;
  let query;

  // Ignore `select` from waterline core
  if (typeof criteria === "object") {
    delete criteria.select;
  }

  // Catch errors build query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch (err) {
    return cb(err);
  }

  try {
    const collection = this.connection.db.collection(self.identity);
    const records = await collection
      .find(query.criteria.where)
      .project({ _id: 1 })
      .toArray();
    // if (records.length < 1) return cb(Errors.NotFound);
    if (!records) return cb(Errors.NotFound); // seems off
    // Build an array of records id
    const deletedRecords = records.map((record) => record._id);
    const deleteResult = await collection.deleteMany(query.criteria.where);
    if (deleteResult.acknowledged) {
      // Force to array to meet Waterline API
      cb(null, utils.rewriteIds(deletedRecords, self.schema));
    }
  } catch (err) {
    if (err) return cb(err);
  }
};

/**
 * Count Documents
 *
 * @param {Object} criteria
 * @param {Function} callback
 * @api public
 */

Collection.prototype.count = async function count(criteria, cb) {
  const self = this;
  let query;

  // Ignore `select` from waterline core
  if (typeof criteria === "object") {
    delete criteria.select;
  }

  // Catch errors build query and return to the callback
  try {
    query = new Query(criteria, this.schema, this.config);
  } catch (err) {
    return cb(err);
  }
  try {
    const count = await this.connection.db
      .collection(this.identity)
      .count(query.criteria.where);
    cb(null, count);
  } catch (err) {
    if (err) return cb(err);
  }
};

/////////////////////////////////////////////////////////////////////////////////
// PRIVATE METHODS
/////////////////////////////////////////////////////////////////////////////////

/**
 * Get name of primary key field for this collection
 *
 * @return {String}
 * @api private
 */
Collection.prototype._getPK = function _getPK() {
  var self = this;
  var pk;

  _.keys(this.schema).forEach(function (key) {
    if (self.schema[key].primaryKey) pk = key;
  });

  if (!pk) pk = "id";
  return pk;
};

/**
 * Parse Collection Definition
 *
 * @param {Object} definition
 * @api private
 */

Collection.prototype._parseDefinition = function _parseDefinition(definition) {
  var self = this;

  // Hold the Schema
  this.schema = _.cloneDeep(definition.definition);

  if (
    _.has(this.schema, "id") &&
    this.schema.id.primaryKey &&
    this.schema.id.type === "integer"
  ) {
    this.schema.id.type = "objectid";
  }

  // Remove any Auto-Increment Keys, Mongo currently doesn't handle this well without
  // creating additional collection for keeping track of the increment values
  Object.keys(this.schema).forEach(function (key) {
    if (self.schema[key].autoIncrement) delete self.schema[key].autoIncrement;
  });

  // Replace any foreign key value types with ObjectId
  Object.keys(this.schema).forEach(function (key) {
    if (self.schema[key].foreignKey) {
      self.schema[key].type = "objectid";
    }
  });

  // Set the identity
  var ident = definition.tableName
    ? definition.tableName
    : definition.identity.toLowerCase();
  this.identity = _.clone(ident);
};

/**
 * Build Internal Indexes Dictionary based on the current schema.
 *
 * @api private
 */

Collection.prototype._buildIndexes = function _buildIndexes() {
  var self = this;

  Object.keys(this.schema).forEach(function (key) {
    var index = {};
    var options = {};

    // If index key is `id` ignore it because Mongo will automatically handle this
    if (key === "id") {
      return;
    }

    // Handle Unique Indexes
    if (self.schema[key].unique) {
      // Set the index sort direction, doesn't matter for single key indexes
      index[key] = 1;

      // Set the index options
      options.sparse = true;
      options.unique = true;

      // Store the index in the collection
      self.indexes.push({ index: index, options: options });
      return;
    }

    // Handle non-unique indexes
    if (self.schema[key].index) {
      // Set the index sort direction, doesn't matter for single key indexes
      index[key] = 1;

      // Set the index options
      options.sparse = true;

      // Store the index in the collection
      self.indexes.push({ index: index, options: options });
      return;
    }
  });
};
