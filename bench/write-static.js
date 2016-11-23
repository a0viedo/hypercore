var hypercore = require('../')
var raf = require('random-access-file')
var zero = require('dev-zero-stream')
var bulk = require('bulk-write-stream')

var feed = hypercore({live: false, reset: true}, function (name) {
  return raf(__dirname + '/static/' + name)
})

var then = Date.now()
var size = 1024 * 1024 * 1024

zero(size).pipe(feed.createWriteStream()).on('finish', function () {
  console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
  console.log(Math.floor(1000 * feed.blocks / (Date.now() - then)) + ' blocks/s')
})
