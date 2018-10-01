
'use strict';
const dse           = require('dse-driver');
const contactPoints = process.env['CONTACT_POINTS'];
const keyspace      = 'ks1';

if (!contactPoints) throw new Error('Environment variable CONTACT_POINTS not set');

const query = 'INSERT INTO temperature (device_id, bucket_id, date, min_value, max_value, values) ' +
              'VALUES (?, ?, ?, ?, ?, ?)';

const client = new dse.Client({
  contactPoints: contactPoints.split(','),
  authProvider: new dse.auth.DsePlainTextAuthProvider('youruser', 'yourpassword'),
  keyspace,
  isMetadataSyncEnabled: false,
  pooling: { warmup: false }
});


module.exports = function(context) {
  context.log("Initializing Azure Function");
  const q = context.req.query;
  const params = [ q.device, q.bucket, new Date(q.date), 
    Number(q.minValue), Number(q.maxValue), q.values.split(',').map(parseFloat) ];
  
  // Execute the query, consider setting prepare to true if Token Aware Routing is required.
  // If preparing the statement, then there is no need to provide the hints for the CQL data type
  const h = ['text', 'text', 'timestamp', 'float', 'float', 'list<float>'];
  client.execute(query, params, { prepare: false, hints: h })
    .catch(err => {
      context.log.error("There was an error executing ", err);
    });
  context.log("Executed query with params: " + params);
  context.done();
};


