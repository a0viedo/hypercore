# hypercore

hypercore backed by SLEEP

```
npm install hypercore
```

## Usage

``` js
var hypercore = require('hypercore')
var raf = require('random-access-file')

var feed = hypercore(function (name) {
  return raf('hypercore/' + name)
})

feed.append('hello')
feed.append('world', function () {
  feed.get(0, console.log)
  feed.get(1, console.log)
})
```

## License

MIT
