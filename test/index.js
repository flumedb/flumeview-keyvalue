
var KV = require('../')
var Flume = require('flumedb')
var Log = require('flumelog-offset')
var tape = require('tape')
var codec = require('flumecodec')

var db = Flume(Log('/tmp/test_flumeview-keyvalue_'+Date.now()+'/log', 1024, codec.json))
  .use('key', KV(1, function (data) {
    return data.key
  }))

tape('simple', function (t) {

  var data = {key: 'foo', value: Math.random()}
  db.append(data, function (err, value) {
    db.key.get('foo', function (err, seq) {
      db.get(seq, function (err, _data) {
        t.deepEqual(_data, data)
        t.end()
      })
    })
  })
})


tape('many', function (t) {
  var batch = []
  for(var i = 0; i < 500; i++)
    batch.push({key: 'k'+i, value:Math.random(), ts: Date.now()})

  db.append(batch, function (err) {
    if(err) throw err
    var i = ~~(Math.random()*batch.length)
    db.key.get(batch[i].key, function (err, seq) {
      if(err) throw err
      db.get(seq, function (err, data) {
        if(err) throw err
        t.deepEqual(data, batch[i])
        t.end()
      })
    })

  })

})
