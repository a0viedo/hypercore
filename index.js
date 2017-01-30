var equals = require('buffer-equals')
var merkle = require('merkle-tree-stream/generator')
var flat = require('flat-tree')
var bulk = require('bulk-write-stream')
var signatures = require('sodium-signatures')
var from = require('from2')
var codecs = require('codecs')
var bitfield = require('sparse-bitfield')
var thunky = require('thunky')
var batcher = require('atomic-batcher')
var inherits = require('inherits')
var events = require('events')
var randomBytes = require('randombytes')
var treeIndex = require('./tree-index')
var storage = require('./storage')
var hash = require('./hash')
var replicate = null

module.exports = Feed

function Feed (key, opts, file) {
  if (!(this instanceof Feed)) return new Feed(key, opts, file)
  events.EventEmitter.call(this)

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

  if (!file) file = opts.storage
  if (!file) throw new Error('You must specify a storage provider')
  if (!opts) opts = {}

  var self = this

  this.id = opts.id || randomBytes(32)
  this.live = opts.live !== false
  this.blocks = 0
  this.bytes = 0
  this.key = key || null
  this.discoveryKey = this.key && hash.discoveryKey(this.key)
  this.secretKey = null
  this.tree = treeIndex(bitfield({trackUpdates: true, pageSize: 1024}))
  this.bitfield = bitfield({trackUpdates: true, pageSize: 1024})
  this.writable = false
  this.readable = true // for completeness
  this.opened = false
  this.ready = thunky(open)

  this._reset = !!opts.reset
  this._merkle = null
  this._storage = storage(file)
  this._batch = batcher(work)

  // Switch to ndjson encoding if JSON is used. That way data files parse like ndjson \o/
  this._codec = codecs(opts.valueEncoding === 'json' ? 'ndjson' : opts.valueEncoding)

  // for replication
  this._selection = []
  this._peers = []

  function work (values, cb) {
    self._append(values, cb)
  }

  function open (cb) {
    self._open(cb)
  }
}

inherits(Feed, events.EventEmitter)

Feed.prototype.replicate = function () {
  // Lazy load replication deps
  if (!replicate) replicate = require('./replicate')
  return replicate(this)
}

Feed.prototype._open = function (cb) {
  var self = this

  this._storage.open(onopen)

  function onopen (err, state) {
    if (err) return cb(err)

    // if no key but we have data do a bitfield reset since we cannot verify the data.
    if (!state.key && (state.treeBitfield.length || state.dataBitfield.length)) {
      self._reset = true
    }

    if (self._reset) {
      state.dataBitfield.fill(0)
      state.treeBitfield.fill(0)
      state.key = state.secretKey = null
    }

    var i = 0

    for (i = 0; i < state.dataBitfield.length; i += 1024) {
      self.bitfield.setBuffer(i, state.dataBitfield.slice(i, i + 1024))
    }
    for (i = 0; i < state.treeBitfield.length; i += 1024) {
      self.tree.bitfield.setBuffer(i, state.treeBitfield.slice(i, i + 1024))
    }

    var len = bitfieldLength(state.treeBitfield)
    if (len) {
      // last node should be a factor of 2 (leaf node)
      // if not, last write wasn't flushed completely and we need to find the
      // last written leaf
      if ((len & 1) === 0) {
        len--
        while (len > 0 && !self.tree.bitfield.get(len - 1)) len -= 2
      }

      if (len > 0) self.blocks = (len + 1) / 2
    }

    if (state.key && self.key && !equals(state.key, self.key)) {
      return cb(new Error('Another hypercore is stored here'))
    }

    if (state.key) self.key = state.key
    if (state.secretKey) self.secretKey = state.secretKey

    if (self.blocks) self._storage.getNode(self.blocks * 2 - 2, onlastnode)
    else onlastnode(null, null)

    function onlastnode (err, node) {
      if (err) return cb(err)

      if (node) self.live = !!node.signature

      if (!self.key && self.live) {
        var keyPair = signatures.keyPair()
        self.secretKey = keyPair.secretKey
        self.key = keyPair.publicKey
      }

      self.writable = !!self.secretKey || self.key === null
      self.discoveryKey = self.key && hash.discoveryKey(self.key)

      var missing = 1 + (self.key ? 1 : 0) + (self.secretKey ? 1 : 0) + (self._reset ? 2 : 0)
      var error = null

      if (self.key) self._storage.key.write(0, self.key, done)
      if (self.secretKey) self._storage.secretKey.write(0, self.secretKey, done)

      if (self._reset) { // TODO: support storage.resize for this instead
        self._storage.treeBitfield.write(0, state.treeBitfield, done)
        self._storage.dataBitfield.write(0, state.dataBitfield, done)
      }

      done(null)

      function done (err) {
        if (err) error = err
        if (--missing) return
        if (error) return cb(error)
        self._roots(self.blocks, onroots)
      }

      function onroots (err, roots) {
        if (err) return cb(err)

        self._merkle = merkle(hash, roots)
        self.bytes = roots.reduce(addSize, 0)
        self.opened = true
        self.emit('ready')

        cb(null)
      }
    }
  }
}

Feed.prototype.proof = function (index, opts, cb) {
  if (typeof opts === 'function') return this.proof(index, null, opts)
  if (!this.opened) return this._readyAndProof(index, opts, cb)
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

Feed.prototype._readyAndProof = function (index, opts, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.proof(index, opts, cb)
  })
}

Feed.prototype.put = function (index, data, proof, cb) {
  if (!this.opened) return this._readyAndPut(index, data, proof, cb)
  this._putBuffer(index, this._codec.encode(data), proof, cb)
}

Feed.prototype._putBuffer = function (index, data, proof, cb) {
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

Feed.prototype._readyAndPut = function (index, data, proof, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.put(index, data, proof, cb)
  })
}

Feed.prototype._commit = function (index, data, nodes, cb) {
  var self = this
  var pending = nodes.length + 1
  var error = null

  for (var i = 0; i < nodes.length; i++) self._storage.putNode(nodes[i].index, nodes[i], ondone)
  self._storage.putData(index, data, nodes, ondone)

  function ondone (err) {
    if (err) error = err
    if (--pending) return

    if (error) return cb(error)
    for (var i = 0; i < nodes.length; i++) self.tree.set(nodes[i].index)
    self.tree.set(2 * index)
    if (self.bitfield.set(index, true)) self.emit('download', index, data)
    self._sync(cb)
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

Feed.prototype.has = function (index) {
  return this.bitfield.get(index)
}

Feed.prototype.get = function (index, cb) {
  if (!this.opened) return this._readyAndGet(index, cb)

  if (!this.has(index)) {
    if (this.writable) return cb(new Error('Block not written'))
    this._selection.push({index: index, cb: cb})
    for (var i = 0; i < this._peers.length; i++) this._peers[i].update()
    return
  }

  if (this._codec !== codecs.binary) cb = this._wrapCodec(cb)
  this._storage.getData(index, cb)
}

Feed.prototype._readyAndGet = function (index, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self.get(index, cb)
  })
}

Feed.prototype._wrapCodec = function (cb) {
  var self = this
  return function (err, buf) {
    if (err) return cb(err)
    cb(null, self._codec.decode(buf))
  }
}

Feed.prototype.createWriteStream = function () {
  var self = this
  return bulk.obj(write)

  function write (batch, cb) {
    self._append(batch, cb)
  }
}

Feed.prototype.createReadStream = function () {
  var self = this
  var start = 0

  return from.obj(read)

  function read (size, cb) {
    if (!self.opened) return open(size, cb)
    if (start === self.blocks) return cb(null, null)
    self.get(start++, cb)
  }

  function open (size, cb) {
    self.ready(function (err) {
      if (err) return cb(err)
      read(size, cb)
    })
  }
}

Feed.prototype.finalize = function (cb) {
  if (!this.key) this.key = hash.tree(this._merkle.roots)
  this._storage.key.write(0, this.key, cb)
}

Feed.prototype.append = function (batch, cb) {
  this._batch(Array.isArray(batch) ? batch : [batch], cb || noop)
}

Feed.prototype.flush = function (cb) {
  this._batch([], cb)
}

Feed.prototype.close = function (cb) {
  var self = this

  this.ready(function () {
    self._storage.close(cb)
  })
}

Feed.prototype._append = function (batch, cb) {
  if (!this.opened) return this._readyAndAppend(batch, cb)

  var self = this
  var pending = batch.length
  var offset = 0
  var error = null

  if (!pending) return cb()

  for (var i = 0; i < batch.length; i++) {
    var data = this._codec.encode(batch[i])
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

    self.bytes += offset // TODO: set after _sync (have a ._bytes prop)
    for (var i = 0; i < batch.length; i++) {
      self.bitfield.set(self.blocks, true)
      self.tree.set(2 * self.blocks++) // TODO: also set after _sync (have a ._blocks prop)
    }

    self._sync(cb)
  }
}

Feed.prototype._readyAndAppend = function (batch, cb) {
  var self = this
  this.ready(function (err) {
    if (err) return cb(err)
    self._append(batch, cb)
  })
}

Feed.prototype._sync = function (cb) { // TODO: mutex it
  var missing = this.bitfield.updates.length + this.tree.bitfield.updates.length
  var next = null
  var error = null

  // All data / nodes have been written now. We still need to update the bitfields though

  // TODO 1: if the program fails during this write the bitfield might not have been fully written
  // HOWEVER, we can easily recover from this by traversing the tree and checking if the nodes exists
  // on disk. So if a get fails, it should try and recover once.

  // TODO 2: if .writable append bitfield updates into a single buffer for extra perf
  // Added benefit is that if the program exits while flushing the bitfield the feed will only get
  // truncated and not have missing chunks which is what you expect.

  while ((next = this.bitfield.nextUpdate()) !== null) {
    this._storage.dataBitfield.write(next.offset, next.buffer, ondone)
  }

  while ((next = this.tree.bitfield.nextUpdate()) !== null) {
    this._storage.treeBitfield.write(next.offset, next.buffer, ondone)
  }

  function ondone (err) {
    if (err) error = err
    if (--missing) return
    cb(error)
  }
}

Feed.prototype._roots = function (index, cb) {
  var roots = flat.fullRoots(2 * index)
  var result = new Array(roots.length)
  var pending = roots.length
  var error = null

  if (!pending) return cb(null, result)

  for (var i = 0; i < roots.length; i++) {
    this._storage.getNode(roots[i], onnode)
  }

  function onnode (err, node) {
    if (err) error = err
    if (node) result[roots.indexOf(node.index)] = node
    if (--pending) return
    if (error) return cb(error)
    cb(null, result)
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

function bitfieldLength (buf) {
  if (!buf.length) return 0

  var max = buf.length - 1
  while (max && !buf[max]) max--

  var b = buf[max]
  if (!b) return max * 8

  var length = (max + 1) * 8

  while (true) {
    if (b & 1) return length
    b >>= 1
    length--
  }
}
