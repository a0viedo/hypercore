var uint64be = require('uint64be')
var flat = require('flat-tree')

module.exports = Storage

var blank = new Buffer(64)
blank.fill(0)

function Storage (create) {
  if (!(this instanceof Storage)) return new Storage(create)

  this.info = create('sleep.info')
  this.tree = create('sleep.tree')
  this.data = create('sleep.data')
  this.dataBitfield = create('sleep.data.bitfield')
  this.treeBitfield = create('sleep.tree.bitfield')
}

Storage.prototype.putBitfield = function (index, part, cb) {
  this.dataBitfield.write(index * 1024, part, cb)
}

Storage.prototype.getBitfield = function (index, cb) {
  this.dataBitfield.read(index * 1024, 1024, cb)
}

Storage.prototype.putData = function (index, data, cb) {
  if (!cb) cb = noop
  var self = this
  if (!data.length) return cb(null)
  this.dataInfo(index, function (err, offset, size) {
    if (err) return cb(err)
    if (size !== data.length) return cb(new Error('Unexpected data size'))
    self.data.write(offset, data, cb)
  })
}

Storage.prototype.getData = function (index, cb) {
  var self = this
  this.dataInfo(index, function (err, offset, size) {
    if (err) return cb(err)
    self.data.read(offset, size, cb)
  })
}

Storage.prototype.dataInfo = function (index, cb) {
  var roots = flat.fullRoots(2 * index)
  var self = this
  var offset = 0
  var pending = roots.length
  var error = null
  if (!pending) this.getNode(2 * index, onlast)

  for (var i = 0; i < roots.length; i++) {
    this.getNode(roots[i], onnode)
  }

  function onlast (err, node) {
    if (err) return cb(err)
    cb(null, offset, node.size)
  }

  function onnode (err, node) {
    if (err) error = err
    if (node) offset += node.size
    if (--pending) return
    if (error) return cb(error)
    self.getNode(2 * index, onlast)
  }
}

Storage.prototype.putInfo = function (info, cb) {
  var buf = new Buffer(72)
  uint64be.encode(info.blocks, buf, 0)
  info.key.copy(buf, 8)
  if (info.secretKey) info.secretKey.copy(buf, 40)
  else blank.copy(buf, 40)
  this.info.write(0, buf, cb)
}

Storage.prototype.getInfo = function (cb) {
  var self = this

  this.info.read(0, 72, function (err, buf) {
    if (err) return cb(err)

    var secretKey = buf.slice(40)

    cb(null, {
      blocks: uint64be.decode(buf, 0),
      key: buf.slice(8, 40),
      secretKey: isNil(secretKey) ? null : secretKey
    })
  })
}

Storage.prototype.getNode = function (index, cb) {
  var leaf = !(index & 1)
  var offset = 40 * index + 64 * Math.ceil(index / 2)
  var length = leaf ? 104 : 40

  this.tree.read(offset, length, function (err, buf) {
    if (err) return cb(err)

    var hash = buf.slice(0, 32)
    var size = uint64be.decode(buf, 32)

    if (!size && isNil(hash)) return cb(new Error('Index not found ' + index + ' '))

    var val = new Node(index, hash, size, leaf ? buf.slice(40) : null)
    cb(null, val)
  })
}

Storage.prototype.putNode = function (index, node, cb) {
  if (!cb) cb = noop
  var leaf = !(index & 1)
  var length = leaf ? 104 : 40
  var offset = 40 * index + 64 * Math.ceil(index / 2)
  var buf = new Buffer(length)

  node.hash.copy(buf, 0)
  uint64be.encode(node.size, buf, 32)
// if (offset === 224) console.log('offset', offset, buf.length, index)
  if (leaf) {
    if (node.signature) node.signature.copy(buf, 40)
    else blank.copy(buf, 40)
  }

  this.tree.write(offset, buf, cb)
}

Storage.Node = Node

function noop () {}

function Node (index, hash, size, sig) {
  this.index = index
  this.hash = hash
  this.size = size
  this.signature = sig
}

function isNil (buf) {
  for (var i = 0; i < buf.length; i++) {
    if (buf[i]) return false
  }
  return true
}
