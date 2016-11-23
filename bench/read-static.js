var raf = require('random-access-file')
var hypercore = require('../')

var feed = hypercore({live: false}, function (name) {
  return raf(__dirname + '/static/' + name)
})

var then = Date.now()
var size = 0
var cnt = 0

feed.createReadStream()
  .on('data', function (data) {
    size += data.length
    cnt++
  })
  .on('end', function () {
    console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
    console.log(Math.floor(1000 * cnt / (Date.now() - then)) + ' blocks/s')
  })
