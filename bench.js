var bench = require('nanobench')

bench('setup test data', function (b) {
  var fs = require('fs')

  var buf = new Buffer(1024 * 1024)
  buf.fill('0123457')

  write(500, function () {
    write(2000, function () {
      b.end()
    })
  })

  function write (size, cb) {
    fs.stat('/tmp/' + size + 'mb', function (err, st) {
      if (!err && st.size === size * 1024 * 1024) return cb()

      var ws = fs.createWriteStream('/tmp/' + size + 'mb')
      var missing = size

      loop(null)

      function loop (err) {
        if (err) return b.error(err)
        if (!missing--) return ws.end(cb)
        ws.write(buf, loop)
      }
    })
  }
})

bench('copy 500mb file (raw fs, baseline)', function (b) {
  var fs = require('fs')

  var rs = fs.createReadStream('/tmp/500mb')
  var ws = fs.createWriteStream('/tmp/output')

  b.start()
  rs.pipe(ws).on('finish', function () {
    b.end()
  })
})

bench.only('index 500mb file (raw fs copy to hypercore)', function (b) {
  var hypercore = require('./')
  var fs = require('fs')
  var raf = require('random-access-file')

  var rs = fs.createReadStream('/tmp/500mb')
  var feed = hypercore(function (name) {
    return raf('/tmp/hypercore/sleep.500.' + name)
  })

  b.start()
  rs.pipe(feed.createWriteStream()).on('finish', function () {
    b.end()
  })
})

bench('copy 2gb file (raw fs, baseline)', function (b) {
  var fs = require('fs')

  var rs = fs.createReadStream('/tmp/2000mb')
  var ws = fs.createWriteStream('/tmp/output')

  b.start()
  rs.pipe(ws).on('finish', function () {
    b.end()
  })
})

bench('index 2gb file', function (b) {
  var hypercore = require('./')
  var fs = require('fs')
  var raf = require('random-access-file')

  var rs = fs.createReadStream('/tmp/2000mb')
  var feed = hypercore(function (name) {
    return raf('/tmp/hypercore/sleep.2000.' + name)
  })

  b.start()
  rs.pipe(feed.createWriteStream()).on('finish', function () {
    b.end()
  })

})
