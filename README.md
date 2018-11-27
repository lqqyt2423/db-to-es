Sync Mongo Data to ES
==========

Sync Mongo(Mongoose) data to ES user bulk api.

## Install

```bash
npm i @liqiqiang/db-to-es --save
```

## Usage

```javascript
const ToEs = require('@liqiqiang/db-to-es');
const cursor = Movie.find({}).cursor();

await new ToEs(cursor, 'es_index_name', 'es_type').start();
```
