var bitfield = require('sparse-bitfield')
var protocol = require('hypercore-protocol')
var set = require('unordered-set')
var remove = require('unordered-array-remove')
var rle = require('bitfield-rle')

module.exports = replicate

function replicate (feed, stream) {
  if (!stream) stream = protocol({id: feed.id})

  var peer = new Peer()

  peer.stream = stream
  peer.feed = feed

  stream.on('close', function () {
    if (peer.channel) peer.destroy()
  })

  feed.ready(function (err) {
    if (stream.destroyed) return
    if (err) return stream.destroy(err)
    if (!feed.key) return stream.destroy(new Error('Finalize static feed before replicating'))

    set.add(feed._peers, peer)
    peer.channel = stream.open(feed.key)
    peer.channel.state = peer

    peer.channel.on('have', onhave)
    peer.channel.on('want', onwant)
    peer.channel.on('request', onrequest)
    peer.channel.on('data', ondata)

    peer.channel.have({
      start: 0,
      bitfield: rle.encode(feed.bitfield.toBuffer())
    })

    peer.update()
  })

  return stream
}

function Peer () {
  this.stream = null
  this.feed = null
  this.channel = null
  this.remoteBitfield = bitfield()
  this.destroyed = false

  this._index = 0 // for the set
  this._reserved = bitfield()
}

Peer.prototype.have = function (message) {
  this.channel.have(message)
}

Peer.prototype.update = function () {
  var selection = this.feed._selection

  for (var i = 0; i < selection.length; i++) {
    var s = selection[i]

    if (!this.remoteBitfield.get(s.index)) continue

    if (this._reserved.get(s.index)) continue
    this._reserved.set(s.index, true)

    this.channel.request({
      block: s.index,
      nodes: this.feed.tree.digest(2 * s.index)
    })
  }
}

Peer.prototype.destroy = function (err) {
  if (this.destroyed) return
  this.destroyed = true
  set.remove(this.feed._peers, this)
  this.stream.destroy(err)
}

function onhave (message) {
  var peer = this.state

  if (message.bitfield) { // TODO: support .start
    var pageSize = peer.remoteBitfield.pageSize
    var bitfield = rle.decode(message.bitfield)

    for (var i = 0; i < bitfield.length; i += pageSize) {
      var page = bitfield.slice(i, i + pageSize)
      if (page.length < pageSize) page = expand(page, pageSize)
      peer.remoteBitfield.setBuffer(i, page)
    }
  } else {
    var start = message.start
    var end = message.end || start + 1
    while (start < end) {
      peer.remoteBitfield.set(start++, true)
    }
  }

  peer.update()
}

function onwant () {

}

function onrequest (data) {
  var peer = this.state

  peer.feed.proof(data.block, {digest: data.nodes}, function (err, proof) {
    if (err) return peer.destroy(err)
    peer.feed._storage.getData(data.block, function (err, buffer) {
      if (err) return peer.destroy(err)

      peer.remoteBitfield.set(data.block, true)
      peer.channel.data({
        block: data.block,
        value: buffer,
        nodes: proof.nodes,
        signature: proof.signature
      })

      peer.feed.emit('upload', data.block, buffer, proof)
    })
  })
}

function ondata (data) {
  var peer = this.state

  peer.feed._putBuffer(data.block, data.value, data, peer, function (err) {
    if (err) return peer.destroy(err)
    drain(peer, data.block)
  })
}

function drain (peer, block) {
  for (var i = 0; i < peer.feed._selection.length; i++) {
    var s = peer.feed._selection[i]
    if (s.index !== block) continue

    remove(peer.feed._selection, i--)

    if (s.get) {
      peer.feed.get(s.index, s.callback)
    } else if (s.callback) {
      s.callback(null)
    }
  }

  peer.update()
}

function expand (buf, length) {
  throw new Error('should expand the buffer with 0s')
}
