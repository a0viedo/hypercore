var uint64be = require('uint64be')
var flat = require('flat-tree')
var alru = require('array-lru')

module.exports = Storage

var noarr = []
var blank = new Buffer(64)
blank.fill(0)

function Storage (create) {
  if (!(this instanceof Storage)) return new Storage(create)

  this.cache = alru(65536, {indexedValues: true})
  this.info = create('sleep.info')
  this.tree = create('sleep.tree')
  this.data = create('sleep.data')
  this.dataBitfield = create('sleep.data.bitfield')
  this.treeBitfield = create('sleep.tree.bitfield')
}

Storage.prototype.putData = function (index, data, nodes, cb) {
  if (!cb) cb = noop
  var self = this
  if (!data.length) return cb(null)
  this.dataOffset(index, nodes, function (err, offset, size) {
    if (err) return cb(err)
    if (size !== data.length) return cb(new Error('Unexpected data size'))
    self.data.write(offset, data, cb)
  })
}

Storage.prototype.getData = function (index, cb) {
  var self = this
  this.dataOffset(index, noarr, function (err, offset, size) {
    if (err) return cb(err)
    self.data.read(offset, size, cb)
  })
}

Storage.prototype.dataOffset = function (index, cachedNodes, cb) {
  var roots = flat.fullRoots(2 * index)
  var self = this
  var offset = 0
  var pending = roots.length
  var error = null

  if (!pending) this.getNode(2 * index, onlast)

  for (var i = 0; i < roots.length; i++) {
    var node = findNode(cachedNodes, roots[i])
    if (node) onnode(null, node)
    else this.getNode(roots[i], onnode)
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
  var buf = new Buffer(105)

  if (info.key) info.key.copy(buf, 0)
  else blank.copy(buf, 0)

  if (info.secretKey) info.secretKey.copy(buf, 32)
  else blank.copy(buf, 32)

  buf[96] = info.live ? 1 : 0
  uint64be.encode(info.blocks, buf, 97)

  this.info.write(0, buf, cb)
}

Storage.prototype.getInfo = function (cb) {
  var self = this

  this.info.read(0, 105, function (err, buf) {
    if (err) return cb(err)

    cb(null, {
      key: notBlank(buf.slice(0, 32)),
      secretKey: notBlank(buf.slice(32, 96)),
      live: !!buf[96],
      blocks: uint64be.decode(buf, 97)
    })
  })
}

Storage.prototype.getNode = function (index, cb) {
  if (this.cache) {
    var cached = this.cache.get(index)
    if (cached) return cb(null, cached)
  }

  var leaf = !(index & 1)
  var offset = 40 * index + 64 * Math.ceil(index / 2)
  var length = leaf ? 104 : 40
  var self = this

  this.tree.read(offset, length, function (err, buf) {
    if (err) return cb(err)

    var hash = buf.slice(0, 32)
    var size = uint64be.decode(buf, 32)

    if (!size && !notBlank(hash)) return cb(new Error('Index not found ' + index + ' '))

    var val = new Node(index, hash, size, leaf ? notBlank(buf.slice(40)) : null)
    if (self.cache) self.cache.set(index, val)
    cb(null, val)
  })
}

Storage.prototype.putNode = function (index, node, cb) {
  if (!cb) cb = noop

  if (this.cache) this.cache.set(index, node)

  var leaf = !(index & 1)
  var length = leaf ? 104 : 40
  var offset = 40 * index + 64 * Math.ceil(index / 2)
  var buf = new Buffer(length)

  node.hash.copy(buf, 0)
  uint64be.encode(node.size, buf, 32)

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

function findNode (nodes, index) {
  for (var i = 0; i < nodes.length; i++) {
    if (nodes[i].index === index) return nodes[i]
  }
  return null
}

function notBlank (buf) {
  for (var i = 0; i < buf.length; i++) {
    if (buf[i]) return buf
  }
  return null
}
