var hypercore = require('../')
var raf = require('random-access-file')
var shuffle = require('shuffle-array')

var source = hypercore(data('64kb'))

source.ready(function () {
  var dest = hypercore(source.key, {storage: data('64kb-copy'), reset: true})
  var s = source.replicate()

  s.pipe(dest.replicate()).pipe(s)

  var then = Date.now()
  var missing = []
  var active = 0
  var size = 0
  var cnt = 0

  for (var i = 0; i < source.blocks; i++) missing.push(i)

  shuffle(missing)
  for (var i = 0; i < 16; i++) copy(null)

  function copy (err, data) {
    if (err) throw err

    if (data) {
      size += data.length
      cnt++
      active--
    }

    if (!missing.length) {
      if (!active) {
        console.log(Math.floor(1000 * size / (Date.now() - then)) + ' bytes/s')
        console.log(Math.floor(1000 * cnt / (Date.now() - then)) + ' blocks/s')
      }
      return
    }

    active++
    dest.get(missing.shift(), copy)
  }
})

function data (folder) {
  return function (name) {
    return raf(__dirname + '/cores/' + folder + '/' + name)
  }
}
