var restify = require('restify');
var static = require('node-static');
var tqs = require('./index');
var bunyan = require('bunyan')
var gelfStream = require('gelf-stream')


/// BUNYAN config
var log = bunyan.createLogger({
    name: 'tqs',
    level: 'debug',
    streams: [
        {
            stream: process.stdout,
            level: process.env.LOG_LEVEL || 'warn',
        }, 
        {
            type: 'rotating-file',
            path: 'tqs.log',
            period: '1d',   // daily rotation
            count: 3        // keep 3 back copies
        }
    ],
    serializers: {
        req: bunyan.stdSerializers.req,
        res: restify.bunyan.serializers.res,
    },
});

var systemProperties = require('./propertyBootstrap').getProperties();

var config = {
  MONGODB_HOST: systemProperties.get('mongodb.host') || 'localhost',
  loggerInstance : log,
  tqsDb: {
      host: 'localhost',
      database: 'tqsTest'
  }
};

tqs.init(config);

var app = restify.createServer();
app.use(restify.acceptParser(app.acceptable));
app.use(restify.dateParser());
app.use(restify.queryParser({
  mapParams: false
}));
app.use(restify.bodyParser({
  mapParams: false
})); // keep body and params seperate
app.on('after', restify.auditLogger({
  log: log
}));

app.pre(function(request, response, next) {
  request.log.info({
    req: request
  }, 'start'); // (1)
  return next();
});


app.on("NotFound", function(req, res) {
  req.addListener('end', function() {
    file.serve(req, res);
  });
});
app.listen(8999);
tqs.listen(app);

