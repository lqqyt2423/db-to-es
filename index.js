'use strict';

const rp = require('request-promise');

// insert not update
// TODO: update
class ToEs {
  constructor(dbCursor, index, type, bulkLimit = 100, host = 'localhost:9200') {
    this.dbCursor = dbCursor;
    this.index = index;
    this.type = type;
    this.bulkLimit = bulkLimit;
    this.host = host;
    // bulk 中转
    this.items = [];
  }

  async start() {
    await new Promise((resolve, reject) => {
      const eachCursor = async () => {
        try {
          const item = await this.dbCursor.next();
          // finish
          if (!item) {
            await this.bulkRequest();
            return resolve();
          }
          this.items.push(item);
          if (this.items.length >= this.bulkLimit) {
            await this.bulkRequest();
          }
        } catch(e) {
          return reject(e);
        }
        eachCursor();
      };
      eachCursor();
    });
  }

  async bulkRequest() {
    if (!this.items.length) return;
    let reqBody = '';
    this.items.forEach(item => {
      const actionStr = JSON.stringify({
        index: {
          _index: this.index,
          _type: this.type,
          _id: item._id,
        }
      });
      reqBody = reqBody + actionStr + '\n';

      const itemStr = JSON.stringify({
        ...item.toObject(),
        _id: undefined,
      });
      reqBody = reqBody + itemStr + '\n';
    });

    const rpOpts = {
      uri: `http://${this.host}/_bulk`,
      body: reqBody,
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
    };

    let res = await rp(rpOpts);
    res = JSON.parse(res);
    const resItems = res.items;

    // log
    console.log(this.index, 'bulk items length:', this.items.length);
    const log = {};
    resItems.forEach(i => {
      const status = String(i.index.status);
      if (status === '400') {
        console.log('400 Error', JSON.stringify(i));
      }
      if (status in log) {
        ++log[status];
      } else {
        log[status] = 1;
      }
    });
    console.log(JSON.stringify(log));

    // 重置
    this.items = [];
  }
}

module.exports = ToEs;
