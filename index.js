'use strict'
var LRU = require('hashlru')
var Obv = require('obv')
var path = require('path')
var createKV = require('flume-kv/level')
var pull = require('pull-stream')

function isString (s) { return 'string' === typeof s }
var isArray = Array.isArray

module.exports = function (version, map) {

  return function (log, name) {
    var kv = createKV(path.join(path.dirname(log.filename), name))
    var cache = LRU(1000)

    function get(key, cb) {
      var v
      //take value from the cache, or from the writing data.
      //can't trust the cache because if there are lots of reads something may have been purged!
      if((v = cache.get(key)) != null || (v = writing[key]) != null || (v = _writing[key]) != null)
        cb(null, v)
      else if(cbs[key])
        cbs[key].push(cb)
      else {
        cbs[key] = [cb]
        kv.get(key, function (err, v) {
          if(!err) cache.set(key, v)
          while(cbs.length)
            cbs.shift()(err, v)
        })
      }
    }

    var since = Obv(), writing = {}, keys = 0

    function add(key, value) {
      if(!writing[key])
        keys ++
      cache.set(key, writing[key] = value)
    }

    kv.load(function (err, v) {
      since.set(err ? -1 : v)
    })

    //id like to modularize stuff like this, but it always seems
    //to have special things relevant to the specific case.
    var write = (function () {
      var ts = 0, isWriting = false, timer

      function __write (cb) {
        var _writing = writing; keys = 0; writing = {}
        kv.batch(_writing, {version: version,  since: since.value}, cb)
      }

      function _write () {
        isWriting = true
        var _since = since.value
        __write(function () {
          isWriting = false
          //if we have new data, maybe write again?
          if(since.value > _since) write()
        })
      }

      return function () {
        if(isWriting) return
        var _ts = Date.now()
        if(keys > 500)
          _write()
        else if (
          since.value === log.since.value &&
          _ts > ts + 60*1000
        ) {
          clearTimeout(timer)
          timer = setTimeout(_write, 200)
        }
      }
    })()

    return {
      methods: {get: 'async'},
      get: get,
      since: since,
      createSink: function (cb) {
        return pull(
          pull.drain(function (data) {
            var indexes = map(data.value, data.seq)

            if(isString(indexes))
              add(indexes, data.seq)
            else if (isArray(indexes))
              indexes.forEach(function (key) { add(key, data.seq) })

            since.set(data.seq)
            write()
          })
        )
      }
    }
  }
}










