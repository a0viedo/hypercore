var hypercore = require('./')
var ram = require('random-access-memory')
var raf = require('random-access-file')

var bulk = require('bulk-write-stream')
var fs = require('fs')

var then = Date.now()
var w = hypercore({valueEncoding: 'json'}, function (name) {
  return raf('tmp/' + name)
})

w.ready(function () {
  console.log('Contains %d blocks and %d bytes (live: %s)\n', w.blocks, w.bytes, w.live)
})

w.createReadStream()
  .on('data', console.log)
  .on('end', console.log.bind(console, '\n(end)'))

w.append({
  hello: 'world'
})

w.append({
  hej: 'verden'
})

w.append({
  hola: 'mundo'
})

w.flush(function () {
  console.log('Appended 3 more blocks')
})
