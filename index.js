'use strict';

var Hapi = require('hapi'),
  fs = require('fs'),
  BPromise = require('bluebird'),
  Boom = require('boom'),
  Joi = require('joi'),
  csvParse = require('csv-parse'),
  streamTransform = require('stream-transform'),
  server = new Hapi.Server();

server.connection({ port: 3000 });

server.route({
  method: 'POST',
  path: '/upload/streamed-processing',
  config: {
    payload: {
      maxBytes: 209715200,
      output: 'stream',
      parse: true
    },
    validate: {
      payload: {
        file: Joi.any()
      }
    }
  },
  handler: function (req, reply) {
    var file = req.payload.file,
      errors = [],
      recordCount = 0;

    console.log('upload started. file:', req.payload.file.hapi.filename);

    var transformer = streamTransform(function(record, callback) {
      if (!record) {
        return callback(null);
      }

      // enable or disable this block to generate an error
      if (record[0] == '223488') {
        var err = new Error('test error');

        err.record = record;
        Object.defineProperty(err, 'message', {
          enumerable: true,
        });

        errors.push(err);
        return callback();
      }

      recordCount++;
      return exports.processRecord(record).nodeify(callback);

    }, {parallel: 10});

    var ex = file.pipe(csvParse()).pipe(transformer);

    transformer.on('error', function (err) {
      reply({
        acknowledged: false,
        filename: file.hapi.filename,
        record_count: recordCount,
        error: err
      });
    });

    transformer.on('finish', function () {
      reply({
        acknowledged: !errors.length,
        filename: file.hapi.filename,
        record_count: recordCount,
        errors: errors
      });
    });

    file.on('end', function () {
      console.log('file end');
    });

    ex.on('data', function (chunk) {
      // absorb data from transform pipe
    });

    ex.on('end', function () {
      console.log('done with pipes');
    });

  }
});


exports.processRecord = function (record) {
  // process record
  return BPromise.resolve(record[0] + '\n');
}

server.start(function () {
  console.log('Server running at:', server.info.uri);
});
