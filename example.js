var hypercore = require('./')
var ram = require('random-access-memory')
var raf = require('random-access-file')

var bulk = require('bulk-write-stream')
var fs = require('fs')

var then = Date.now()
var w = hypercore(function (name) {
  return raf('tmp/' + name)
})

w.append(JSON.stringify({
  hello: 'world'
}) + '\n')

w.append(JSON.stringify({
  hej: 'verden'
}) + '\n')

w.append([JSON.stringify({
  hola: 'mundo'
}) + '\n'])

w.flush(function () {
  console.log('??', w.blocks, w.bytes)
})

// w.open(function () {
//   console.log('opened')
// })

return

var rs = fs.createReadStream('/Users/maf/Downloads/tears_of_steel_1080p.webm')
// var rs = fs.createReadStream('/Users/maf/Downloads/win7.iso')
// var rs = fs.createReadStream('/Users/maf/Downloads/video.webm')
// var rs = fs.createReadStream(__filename)
var ws = w.createWriteStream()
var total = 0

rs.pipe(ws).on('finish', function () {
  console.log('bytes/ms', w.bytes / (Date.now() - then))
  console.log('blocks', w.blocks)

  w.finalize(function () {
    console.log('key', w.key.toString('hex'))
    // return
    var r = hypercore(w.key, function (name) {
      return raf('/tmp/hypercore-clone/' + name)
    })


    // copy(w, r, 536, console.log)

    // return

    var missing = []
    for (var i = 0; i < w.blocks; i++) missing.push(i)
    missing = require('shuffle-array')(missing)

    var then = Date.now()

    loop()

    function loop (err) {
      if (err) throw err
      if (!missing.length) return done()
      copy(w, r, missing.shift(), loop)
    }

    function done () {
      console.log('done!')
      console.log('bytes/ms', w.bytes / (Date.now() - then))
      console.log('from: %d blocks, %d bytes', w.blocks, w.bytes)
      console.log('to: %d blocks, %d bytes', r.blocks, r.bytes)
    }
  })
})

function copy (from, to, index, cb) {
  from.proof(index, {tree: to.tree}, function (err, proof) {
    if (err) return cb(err)
    total += proof.nodes.length
    // console.log('index: %d, proof -> {nodes: %d, signature: %s}, total-nodes: %d', index, proof.nodes.length, !!proof.signature, total)
    from.get(index, function (err, data) {
      if (err) return cb(err)
      to.put(index, data, proof, cb)
    })
  })
}
