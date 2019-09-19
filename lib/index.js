'use strict';

Object.defineProperty(exports, "__esModule", {
  value: true
});

var _slicedToArray = function () { function sliceIterator(arr, i) { var _arr = []; var _n = true; var _d = false; var _e = undefined; try { for (var _i = arr[Symbol.iterator](), _s; !(_n = (_s = _i.next()).done); _n = true) { _arr.push(_s.value); if (i && _arr.length === i) break; } } catch (err) { _d = true; _e = err; } finally { try { if (!_n && _i["return"]) _i["return"](); } finally { if (_d) throw _e; } } return _arr; } return function (arr, i) { if (Array.isArray(arr)) { return arr; } else if (Symbol.iterator in Object(arr)) { return sliceIterator(arr, i); } else { throw new TypeError("Invalid attempt to destructure non-iterable instance"); } }; }();

var _createClass = function () { function defineProperties(target, props) { for (var i = 0; i < props.length; i++) { var descriptor = props[i]; descriptor.enumerable = descriptor.enumerable || false; descriptor.configurable = true; if ("value" in descriptor) descriptor.writable = true; Object.defineProperty(target, descriptor.key, descriptor); } } return function (Constructor, protoProps, staticProps) { if (protoProps) defineProperties(Constructor.prototype, protoProps); if (staticProps) defineProperties(Constructor, staticProps); return Constructor; }; }();

exports.default = init;

var _uberproto = require('uberproto');

var _uberproto2 = _interopRequireDefault(_uberproto);

var _feathersQueryFilters = require('feathers-query-filters');

var _feathersQueryFilters2 = _interopRequireDefault(_feathersQueryFilters);

var _feathersErrors = require('feathers-errors');

var _feathersErrors2 = _interopRequireDefault(_feathersErrors);

var _feathersCommons = require('feathers-commons');

var _parse = require('./parse');

function _interopRequireDefault(obj) { return obj && obj.__esModule ? obj : { default: obj }; }

function _defineProperty(obj, key, value) { if (key in obj) { Object.defineProperty(obj, key, { value: value, enumerable: true, configurable: true, writable: true }); } else { obj[key] = value; } return obj; }

function _toConsumableArray(arr) { if (Array.isArray(arr)) { for (var i = 0, arr2 = Array(arr.length); i < arr.length; i++) { arr2[i] = arr[i]; } return arr2; } else { return Array.from(arr); } }

function _classCallCheck(instance, Constructor) { if (!(instance instanceof Constructor)) { throw new TypeError("Cannot call a class as a function"); } }

var BASE_EVENTS = ['created', 'updated', 'patched', 'removed'];

// Create the service.

var Service = function () {
  function Service(options) {
    _classCallCheck(this, Service);

    if (!options) {
      throw new Error('RethinkDB options have to be provided.');
    }

    if (options.Model) {
      options.r = options.Model;
    } else {
      throw new Error('You must provide the RethinkDB object on options.Model');
    }

    if (!options.r._poolMaster._options.db) {
      throw new Error('You must provide an instance of r that is preconfigured with a db. You can override the db for the current query by specifying db: in service options');
    }

    // if no options.db on service use default from pool master
    if (!options.db) {
      options.db = options.r._poolMaster._options.db;
    }

    if (!options.name) {
      throw new Error('You must provide a table name on options.name');
    }

    this.type = 'rethinkdb';
    this.id = options.id || 'id';
    this.table = options.r.db(options.db).table(options.name);
    this.options = options;
    this.watch = options.watch !== undefined ? options.watch : true;
    this.paginate = options.paginate || {};
    this.events = this.watch ? BASE_EVENTS.concat(options.events) : options.events || [];
  }

  _createClass(Service, [{
    key: 'extend',
    value: function extend(obj) {
      return _uberproto2.default.extend(obj, this);
    }
  }, {
    key: 'init',
    value: function init() {
      var opts = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};

      var r = this.options.r;
      var t = this.options.name;
      var db = this.options.db;

      return r.dbList().contains(db) // create db if not exists
      .do(function (dbExists) {
        return r.branch(dbExists, { created: 0 }, r.dbCreate(db));
      }).run().then(function () {
        return r.db(db).tableList().contains(t) // create table if not exists
        .do(function (tableExists) {
          return r.branch(tableExists, { created: 0 }, r.db(db).tableCreate(t, opts));
        }).run();
      });
    }
  }, {
    key: 'createFilter',
    value: function createFilter(query) {
      return (0, _parse.createFilter)(query, this.options.r);
    }
  }, {
    key: 'createQuery',
    value: function createQuery(originalQuery) {
      var _filter = (0, _feathersQueryFilters2.default)(originalQuery || {}),
          filters = _filter.filters,
          query = _filter.query;

      var r = this.options.r;

      var rq = this.table;
      if (query.id) {
        var _rq;

        var $in = query.id.$in;
        rq = $in ? (_rq = rq).getAll.apply(_rq, _toConsumableArray($in)) : rq.get(query.id);
        delete query.id;
      } else if (filters.$getAll) {
        _feathersCommons._.each(filters.$getAll, function (values, fieldName) {
          rq = rq.getAll(r.args(values), { index: fieldName });
        });
      }

      rq = rq.filter(this.createFilter(query));

      // Handle $select
      if (filters.$select) {
        rq = rq.pluck(filters.$select);
      }

      // Handle $omit
      if (filters.$omit) {
        rq = rq.without(filters.$omit);
      }

      // Handle $sort
      if (filters.$sort) {
        var _rq2;

        var sorts = Object.entries(filters.$sort).map(function (_ref) {
          var _ref2 = _slicedToArray(_ref, 2),
              fieldName = _ref2[0],
              order = _ref2[1];

          return parseInt(order) === 1 ? function (row) {
            return r.branch(row.hasFields(fieldName), row(fieldName), 'ZZZZZ');
          } : r.desc(fieldName);
        });
        if (filters.$sortI) sorts.push({ index: filters.$sortI });
        rq = (_rq2 = rq).orderBy.apply(_rq2, _toConsumableArray(sorts));
      }
    }
  }, {
    key: '_find',
    value: function _find() {
      var params = arguments.length > 0 && arguments[0] !== undefined ? arguments[0] : {};

      var paginate = typeof params.paginate !== 'undefined' ? params.paginate : this.paginate;
      // Prepare the special query params.

      var _filter2 = (0, _feathersQueryFilters2.default)(params.query || {}, paginate),
          filters = _filter2.filters;

      var q = params.rethinkdb || this.createQuery(params.query);
      var countQuery = void 0;

      // For pagination, count has to run as a separate query, but without limit.
      if (paginate.default) {
        countQuery = q.count().run();
      }

      // Handle $skip AFTER the count query but BEFORE $limit.
      if (filters.$skip) {
        q = q.skip(filters.$skip);
      }
      // Handle $limit AFTER the count query and $skip.
      if (typeof filters.$limit !== 'undefined') {
        q = q.limit(filters.$limit);
      }

      // Execute the query
      return Promise.all([q, countQuery]).then(function (_ref3) {
        var _ref4 = _slicedToArray(_ref3, 2),
            data = _ref4[0],
            total = _ref4[1];

        if (paginate.default) {
          return {
            total: total,
            data: data,
            limit: filters.$limit,
            skip: filters.$skip || 0
          };
        }

        return data;
      });
    }
  }, {
    key: 'find',
    value: function find() {
      return this._find.apply(this, arguments);
    }
  }, {
    key: '_get',
    value: function _get(id) {
      var params = arguments.length > 1 && arguments[1] !== undefined ? arguments[1] : {};

      var query = this.table.filter(params.query).limit(1);

      // If an id was passed, just get the record.
      if (id !== null && id !== undefined) {
        query = this.table.get(id);
      }

      if (params.query && params.query.$select) {
        query = query.pluck(params.query.$select.concat(this.id));
      }

      // Handle $omit
      if (params.query && params.query.$omit) {
        query = query.without(params.query.$omit);
      }

      return query.run().then(function (data) {
        if (Array.isArray(data)) {
          data = data[0];
        }
        if (!data) {
          throw new _feathersErrors2.default.NotFound('No record found for id \'' + id + '\'');
        }
        return data;
      });
    }
  }, {
    key: 'get',
    value: function get() {
      return this._get.apply(this, arguments);
    }
  }, {
    key: 'create',
    value: function create(data, params) {
      var idField = this.id;
      return this.table.insert(data, params.rethinkdb).run().then(function (res) {
        if (data[idField]) {
          if (res.errors) {
            return Promise.reject(new _feathersErrors2.default.Conflict('Duplicate primary key', res.errors));
          }
          return data;
        } else {
          // add generated id
          var addId = function addId(current, index) {
            if (res.generated_keys && res.generated_keys[index]) {
              return Object.assign({}, current, _defineProperty({}, idField, res.generated_keys[index]));
            }

            return current;
          };

          if (Array.isArray(data)) {
            return data.map(addId);
          }

          return addId(data, 0);
        }
      }).then((0, _feathersCommons.select)(params, this.id));
    }
  }, {
    key: 'patch',
    value: function patch(id, data, params) {
      var _this = this;

      var query = void 0;

      if (id !== null && id !== undefined) {
        query = this._get(id);
      } else if (params) {
        query = this._find(params);
      } else {
        return Promise.reject(new Error('Patch requires an ID or params'));
      }

      // Find the original record(s), first, then patch them.
      return query.then(function (getData) {
        var query = void 0;
        var options = Object.assign({ returnChanges: true }, params.rethinkdb);

        if (Array.isArray(getData)) {
          var _table;

          query = (_table = _this.table).getAll.apply(_table, _toConsumableArray(getData.map(function (item) {
            return item[_this.id];
          })));
        } else {
          query = _this.table.get(id);
        }

        return query.update(data, options).run().then(function (response) {
          var changes = response.changes.map(function (change) {
            return change.new_val;
          });
          return changes.length === 1 ? changes[0] : changes;
        });
      }).then((0, _feathersCommons.select)(params, this.id));
    }
  }, {
    key: 'update',
    value: function update(id, data, params) {
      var _this2 = this;

      var options = Object.assign({ returnChanges: true }, params.rethinkdb);

      if (Array.isArray(data) || id === null) {
        return Promise.reject('Not replacing multiple records. Did you mean `patch`?');
      }

      return this._get(id, params).then(function (getData) {
        data[_this2.id] = id;
        return _this2.table.get(getData[_this2.id]).replace(data, options).run().then(function (result) {
          return result.changes && result.changes.length ? result.changes[0].new_val : data;
        });
      }).then((0, _feathersCommons.select)(params, this.id));
    }
  }, {
    key: 'remove',
    value: function remove(id, params) {
      var query = void 0;
      var options = Object.assign({ returnChanges: true }, params.rethinkdb);

      // You have to pass id=null to remove all records.
      if (id !== null && id !== undefined) {
        query = this.table.get(id);
      } else if (id === null) {
        query = this.createQuery(params.query);
      } else {
        return Promise.reject(new Error('You must pass either an id or params to remove.'));
      }

      return query.delete(options).run().then(function (res) {
        if (res.changes && res.changes.length) {
          var changes = res.changes.map(function (change) {
            return change.old_val;
          });
          return changes.length === 1 ? changes[0] : changes;
        } else {
          return [];
        }
      }).then((0, _feathersCommons.select)(params, this.id));
    }
  }, {
    key: 'setup',
    value: function setup() {
      var _this3 = this;

      if (this.watch) {
        this._cursor = this.table.changes().run().then(function (cursor) {
          cursor.each(function (error, data) {
            if (error || typeof _this3.emit !== 'function') {
              return;
            }
            if (data.old_val === null) {
              _this3.emit('created', data.new_val);
            } else if (data.new_val === null) {
              _this3.emit('removed', data.old_val);
            } else {
              _this3.emit('updated', data.new_val);
              _this3.emit('patched', data.new_val);
            }
          });

          return cursor;
        });
      }
    }
  }]);

  return Service;
}();

function init(options) {
  return new Service(options);
}

init.Service = Service;
module.exports = exports['default'];