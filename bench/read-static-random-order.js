var raf = require('random-access-file')
var hypercore = require('../')

var feed = hypercore({live: false}, function (name) {
  return raf(__dirname + '/static/' + name)
})

var then = Date.now()
var size = 0
var cnt = 0

feed.ready(function () {
  var missing = feed.blocks
  var reading = 0

  for (var i = 0; i < 16; i++) read(null, null)

  function read (err, data) {
    if (err) throw err

    if (data) {
      reading--
      cnt++
      size += data.length
    }

    if (!missing) {
      if (!reading) {
        console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
        console.log(Math.floor(1000 * cnt / (Date.now() - then)) + ' blocks/s')
      }
      return
    }

    missing--
    reading++

    var block = Math.floor(Math.random() * feed.blocks)
    feed.get(block, read)
  }
})
