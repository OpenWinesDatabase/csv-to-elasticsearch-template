const elasticsearch = require('elasticsearch');
const csv = require('csv-parser');
const fs = require('fs');

const ES_URI = process.env.ES_URI || 'http://localhost:9200';
console.log(`Using Elastic URI: ${ES_URI}`);

const esClient = new elasticsearch.Client({
  host: ES_URI,
  log: 'error',
  requestTimeout: Infinity
});

const INDEX = process.env.INDEX || 'items';
const TYPE = process.env.TYPE || 'item';

const importItems = (file) => {
  let items = [];
  fs.createReadStream(file)
      .pipe(csv({
        separator: ';',
        headers: ['id', 'name']
      }))
      .on('data', data => {
        // Bulk statement
        items.push({ "index" : { "_index" : INDEX, "_type" : TYPE, "_id" : data['id'] } });
        const item = {
          "@timestamp" : new Date(),
          "name": data['name']
        };
        items.push(item);
        if (items.length > 500) {
            esClient.bulk({
                body: items
            }, (err, resp) => {
                if (err) { throw err; }
                console.log(`${resp.items.length} items inserted from ${file}`);
            });
            items = [];
        }
      })
      .on('end', () => {
        esClient.bulk({
          body: items
        }, (err, resp) => {
          if (err) { throw err; }
          console.log(`${resp.items.length} items inserted from ${file}`);
        });
      });
}

// Import all data!
const dataDir = 'data';
fs.readdir(dataDir, (err, files) => {
  if (err) { throw err; }
  files.forEach(file => {
      console.log(`Importing items from ${file}`)
      importItems(`${dataDir}/${file}`)
  });
})
