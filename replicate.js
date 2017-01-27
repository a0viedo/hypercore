var bitfield = require('sparse-bitfield')
var protocol = require('hypercore-protocol')
var rle = require('bitfield-rle')

module.exports = replicate

function replicate (feed) {
  var stream = protocol({id: feed.id})
  var peer = new Peer()

  peer.stream = stream
  peer.feed = feed

  feed.ready(function () {
    peer.channel = stream.open(feed.key)
    peer.channel.state = peer

    peer.channel.on('have', onhave)
    peer.channel.on('want', onwant)
    peer.channel.on('request', onrequest)
    peer.channel.on('data', ondata)

    feed._peers.push(peer)
    peer.channel.have({
      start: 0,
      bitfield: rle.encode(feed.bitfield.toBuffer())
    })
  })

  return stream
}

function Peer () {
  this.stream = null
  this.feed = null
  this.channel = null
  this.remoteBitfield = bitfield()
  this.remoteTree = bitfield()

  this.remoteRequests = []
  this.responses = []
}

Peer.prototype.update = function () {
  var selection = this.feed._selection

  for (var i = 0; i < selection.length; i++) {
    var s = selection[i]
    if (!this.remoteBitfield.get(s.index)) continue

    this.channel.request({
      block: s.index,
      nodes: this.feed.tree.digest(2 * s.index)
    })
  }
}

Peer.prototype.destroy = function (err) {
  console.log('should destroy the stream', err)
}

function onhave (message) {
  var peer = this.state
  var pageSize = peer.remoteBitfield.pageSize
  var bitfield = rle.decode(message.bitfield)

  for (var i = 0; i < bitfield.length; i += pageSize) {
    var page = bitfield.slice(i, i + pageSize)
    if (page.length < pageSize) page = expand(page, pageSize)
    peer.remoteBitfield.setBuffer(i, page)
  }

  peer.update()
}

function onwant () {

}

function read (peer) {
  var next = peer.remoteRequests[0]

  peer.feed._storage.getData(next.block, function (err, data) {
    if (err) return peer.destroy(err)
    peer.feed.proof(next.block, {digest: next.nodes, tree: peer.remoteTree}, function (err, proof) {
      if (err) return peer.destroy(err)

      peer.channel.data({
        block: next.block,
        value: data,
        nodes: proof.nodes,
        signature: proof.signature
      })

      peer.remoteRequests.shift()
      if (peer.remoteRequests.length) read(peer)
    })
  })
}

function write (peer) {
  var next = peer.responses[0]

  peer.feed._putBuffer(next.block, next.value, next, function (err) {
    if (err) return peer.destroy(err)

    for (var i = 0; i < peer.feed._selection.length; i++) {
      var s = peer.feed._selection[i]

      if (peer.feed.has(s.index)) {
        peer.feed._selection.splice(i--, 1)
        peer.feed.get(s.index, s.cb)
      }
    }

    peer.responses.shift()
    if (peer.responses.length) write(peer)
  })
}

function onrequest (data) {
  // TODO: if "too many" requests are pending - ignore or error
  var peer = this.state

  peer.remoteRequests.push(data)
  if (peer.remoteRequests.length === 1) read(peer)
}

function ondata (data) {
  // TODO: if "too many" responses are pending - ignore or error
  var peer = this.state

  peer.responses.push(data)
  if (peer.responses.length === 1) write(peer)
}

function expand (buf, length) {
  throw new Error('should expand the buffer with 0s')
}
