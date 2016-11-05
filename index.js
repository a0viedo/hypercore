var equals = require('buffer-equals')
var merkle = require('merkle-tree-stream/generator')
var flat = require('flat-tree')
var bulk = require('bulk-write-stream')
var signatures = require('sodium-signatures')
var from = require('from2')
var bitfield = require('sparse-bitfield')
var thunky = require('thunky')
var batcher = require('atomic-batcher')
var treeIndex = require('./tree-index')
var storage = require('./storage')
var hash = require('./hash')

module.exports = Feed

function Feed (key, opts, file) {
  if (!(this instanceof Feed)) return new Feed(key, opts, file)

  if (typeof key === 'string') key = new Buffer(key, 'hex')

  if (typeof key === 'function' || isObject(key)) {
    file = opts
    opts = key
    key = null
  }
  if (typeof opts === 'function') {
    file = opts
    opts = null
  }

  if (!file) throw new Error('You must specify a storage provider')
  if (!opts) opts = {}

  var self = this

  this.live = opts.live !== false
  this.blocks = 0
  this.bytes = 0
  this.key = key || null
  this.secretKey = null
  this.tree = treeIndex()
  this.bitfield = bitfield()
  this.open = thunky(open)

  this._storage = storage(file)
  this._merkle = merkle(hash)
  this._batch = batcher(work)

  if (!this.key && this.live) {
    var pair = signatures.keyPair()
    this.secretKey = pair.secretKey
    this.key = pair.publicKey
  }

  function work (values, cb) {
    self._append(values, cb)
  }

  function open (cb) {
    self._open(cb)
  }
}

Feed.prototype.replicate = function () {
  return null
}

Feed.prototype._open = function (cb) {
  // console.log('sup')
  cb()
}

Feed.prototype.proof = function (index, opts, cb) {
  if (typeof opts === 'function') return this.proof(index, null, opts)
  if (!opts) opts = {}

  var proof = this.tree.proof(2 * index, opts)
  var needsSig = this.live && !!proof.verifiedBy
  var sigIndex = needsSig ? proof.verifiedBy - 2 : 0
  var pending = proof.nodes.length + (needsSig ? 1 : 0)
  var error = null
  var signature = null
  var nodes = new Array(proof.nodes.length)

  if (!pending) return cb(null, {nodes: nodes, signature: null})

  for (var i = 0; i < proof.nodes.length; i++) {
    this._storage.getNode(proof.nodes[i], onnode)
  }

  if (needsSig) {
    this._storage.getNode(sigIndex, onnode)
  }

  function onnode (err, node) {
    if (err) error = err

    if (node) {
      if (needsSig && !signature && node.index === sigIndex) {
        signature = node.signature
      } else {
        nodes[proof.nodes.indexOf(node.index)] = node
      }
    }

    if (--pending) return
    if (error) return cb(error)
    cb(null, {nodes: nodes, signature: signature})
  }
}

Feed.prototype.put = function (index, data, proof, cb) {
  var self = this
  var trusted = -1
  var missing = []
  var next = 2 * index
  var i = 0

  for (i = 0; i < proof.nodes.length; i++) {
    if (this.tree.get(next)) {
      trusted = next
      break
    }

    var sib = flat.sibling(next)
    next = flat.parent(next)

    if (proof.nodes[i].index === sib) continue
    if (!this.tree.get(sib)) break

    missing.push(sib)
  }

  if (trusted === -1 && this.tree.get(next)) trusted = next

  var error = null
  var trustedNode = null
  var missingNodes = new Array(missing.length)
  var pending = missing.length + (trusted > -1 ? 1 : 0)

  for (i = 0; i < missing.length; i++) this._storage.getNode(missing[i], onmissing)
  if (trusted > -1) this._storage.getNode(trusted, ontrusted)
  if (!missing.length && trusted === -1) onmissingloaded(null)

  function ontrusted (err, node) {
    if (err) error = err
    if (node) trustedNode = node
    if (!--pending) onmissingloaded(error)
  }

  function onmissing (err, node) {
    if (err) error = err
    if (node) missingNodes[missing.indexOf(node.index)] = node
    if (!--pending) onmissingloaded(error)
  }

  function onmissingloaded (err) {
    if (err) return cb(err)
    var writes = self._verify(index, data, proof, missingNodes, trustedNode)
    if (!writes) return cb(new Error('Could not verify data'))
    self._commit(index, data, writes, cb)
  }
}

Feed.prototype._commit = function (index, data, nodes, cb) {
  var self = this
  var pending = nodes.length + 1
  var error = null

  for (var i = 0; i < nodes.length; i++) this._storage.putNode(nodes[i].index, nodes[i], ondone)
  this._storage.putData(index, data, ondone)

  function ondone (err) {
    if (err) error = err
    if (--pending) return

    if (error) return cb(err)
    for (var i = 0; i < nodes.length; i++) self.tree.set(nodes[i].index)
    self.tree.set(2 * index)
    self.bitfield.set(index, true)
    cb(null)
  }
}

Feed.prototype._verifyRoots = function (top, proof, batch) {
  var lastNode = proof.nodes.length ? proof.nodes[proof.nodes.length - 1].index : top.index
  var verifiedBy = Math.max(flat.rightSpan(top.index), flat.rightSpan(lastNode)) + 2
  var indexes = flat.fullRoots(verifiedBy)
  var roots = new Array(indexes.length)

  for (var i = 0; i < roots.length; i++) {
    if (indexes[i] === top.index) {
      roots[i] = top
      batch.push(top)
    } else if (proof.nodes.length && indexes[i] === proof.nodes[0].index) {
      roots[i] = proof.nodes.shift()
      batch.push(roots[i])
    } else {
      return null
    }
  }

  var checksum = hash.tree(roots)

  if (proof.signature) {
    // check signature
    if (!signatures.verify(checksum, proof.signature, this.key)) return null
    this.live = true
  } else {
    // check tree root
    if (!equals(checksum, this.key)) return null
    this.live = false
  }

  var blocks = verifiedBy / 2
  if (blocks > this.blocks) {
    this.blocks = blocks
    this.bytes = roots.reduce(addSize, 0)
  }

  return batch
}

Feed.prototype._verify = function (index, data, proof, missing, trusted) {
  var top = new storage.Node(2 * index, hash.data(data), data.length, null)
  var writes = []

  if (verifyNode(trusted, top)) return writes

  while (true) {
    var node = null
    var next = flat.sibling(top.index)

    if (proof.nodes.length && proof.nodes[0].index === next) {
      node = proof.nodes.shift()
      writes.push(node)
    } else if (missing.length && missing[0].index === next) {
      node = missing.shift()
    } else { // all remaining nodes should be roots now
      return this._verifyRoots(top, proof, writes)
    }

    writes.push(top)
    top = new storage.Node(flat.parent(top.index), hash.parent(top, node), top.size + node.size, null)

    if (verifyNode(trusted, top)) return writes
  }
}

Feed.prototype.get = function (i, cb) {
  this._storage.getData(i, cb)
}

Feed.prototype.createWriteStream = function () {
  var self = this
  return bulk(write)

  function write (batch, cb) {
    self._append(batch, cb)
  }
}

Feed.prototype.createReadStream = function () {
  var self = this
  var start = 0
  return from(read)

  function read (size, cb) {
    self.get(start++, cb)
  }
}

Feed.prototype.finalize = function (cb) {
  if (!this.key) this.key = hash.tree(this._merkle.roots)
  this._storage.putInfo({
    blocks: this.blocks,
    key: this.key,
    secretKey: this.secretKey
  }, cb)
}

Feed.prototype.append = function (batch, cb) {
  if (Array.isArray(batch)) this._batch(batch, cb)
  else this._batch([toBuffer(batch)], cb)
}

Feed.prototype.flush = function (cb) {
  this._batch([], cb)
}

Feed.prototype._append = function (batch, cb) {
  if (!cb) cb = noop

  var self = this
  var pending = batch.length
  var offset = 0
  var error = null

  if (!pending) return cb()

  for (var i = 0; i < batch.length; i++) {
    var data = toBuffer(batch[i])
    var nodes = this._merkle.next(data)

    pending += nodes.length
    this._storage.data.write(this.bytes + offset, data, onnode)
    offset += data.length

    for (var j = 0; j < nodes.length; j++) {
      var node = nodes[j]
      // TODO: this might deopt? pass in constructor to the merklelizer
      if (this.live) node.signature = signatures.sign(hash.tree(this._merkle.roots), this.secretKey)
      this._storage.putNode(node.index, node, onnode)
    }
  }

  function onnode (err) {
    if (err) error = err
    if (--pending) return
    if (error) return cb(error)

    self.bytes += offset
    for (var i = 0; i < batch.length; i++) {
      self.bitfield.set(self.blocks, true)
      self.tree.set(2 * self.blocks++)
    }

    cb()
  }
}

function noop () {}

function verifyNode (trusted, node) {
  return trusted && trusted.index === node.index && equals(trusted.hash, node.hash)
}

function addSize (size, node) {
  return size + node.size
}

function isObject (val) {
  return !!(val && typeof val === 'object' && !Buffer.isBuffer(val))
}

function toBuffer (val) {
  if (Buffer.isBuffer(val)) return val
  if (typeof val === 'string') return new Buffer(val)
  throw new Error('Must be a string or buffer')
}
