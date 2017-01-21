
require('level')
  (process.argv[2], {keyEncoding: 'utf8', valueEncoding: 'json', createIfMissing: false})
.createReadStream().on('data', console.log)
