
'use strict';

const dse           = require('dse-driver');
const contactPoints = process.env['CONTACT_POINTS'];
const keyspace      = process.env['KEYSPACE'];

const client        = new dse.Client({
  contactPoints: contactPoints.split(','), keyspace, 
  isMetadataSyncEnabled: false, pooling: { warmup: false }
});

const query = 'INSERT INTO temperature (device_id, bucket_id, date, min_value, max_value, values) ' +
              'VALUES (?, ?, ?, ?, ?, ?)';

// Start connecting to the cluster
client.connect()
  .then(() => client.metadata.refreshKeyspace(keyspace))
  .then(() => console.log('Connected to the DSE cluster, discovered %d nodes', client.hosts.length))
  .catch(err => console.error('There was an error trying to connect', err));

/** 
 *Lambda function entry point. 
 */
exports.handler = (event, context, callback) => {
  
  // Connections are pooled during the lifetime of the Lambda function instance
  context.callbackWaitsForEmptyEventLoop = false;

  // Parameters
  const params = [ event.device, event.bucket, 
      new Date(event.date), event.minValue, event.maxValue, event.values ];

  client.execute(query, params, { prepare: true })
    .then(() => callback())
    .catch(err => {
      console.error("There was an error executing ", err);
      callback(err);
    });
};


