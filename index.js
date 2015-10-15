var tqs = require('./lib/tqslib.js');

var CONFIG;
var logger;

exports.init = function(config) {
  CONFIG = config;
  logger = config.loggerInstance;
  tqs.init(config, logger);
  logger.info("*********** TQS Service Started *****************");
};
//// SERVER SET UP
exports.listen = function(server, io) {

    //publish timer queue item to queue
    server.post({
        path: '/tqs/timer-mgmt/timers',
        name: 'createTimer'
    }, function(req, res, next) {

        var timerData = req.body;
        if (!timerData.schedule && (timerData.timerType === 'cronexpression' || 
                                    timerData.timerType === 'textexpression')) {

            res.send(422, "Schedule is required for cron / text expression");
            next();

        } else {
            tqs.publishTimer(timerData, function(err, object) {
                if (err) {
                    if (err.message && err.message.match(/^422/)) {
                        res.send(422, err.message);
                    } else {
                        res.send(500, err);
                    }
                } else {
                    res.send(200, object);
                }
                next();
            });
        }
    }); 

    server.get({
        path: '/tqs/timer-mgmt/timers',
        name: 'searchTimers'
    }, function(req, res, next) {
        var timerData = req.query;
        tqs.searchTimers(timerData, function(err, object) {
            if (err) {
                res.send(500, err);
            } else {
                res.send(200, object);
            }
            next();
        });
    }); 

    server.get({
        path: '/tqs/timer-mgmt/timers/:timerid',
        name: 'getTimerById'
    }, function(req, res, next) {
        tqs.findTimerById(req.params.timerid, function(err, timerObject) {
            if (err) {
                res.send(500, err);
            } else if (timerObject) {
                res.send(timerObject);
            } else {
                res.send(404, "ENTRY NOT FOUND");
            }
            next();
        });
    });

    server.get({
        path: '/tqs/timer-mgmt/timers/:timerid/audits',
        name: 'getTimerAuditsByTimerId'
    }, function(req, res, next) {
        tqs.getTimerAuditsByTimerId(req.params.timerid, req.query, function(err, audits) {
            if (err) {
                res.send(500, err);
            } else if (audits) {
                res.send(timerObject);
            } else {
                res.send(404, "ENTRY NOT FOUND");
            }
            next();
        });
    });

    server.put({
        path: '/tqs/timer-mgmt/timers/:timerid',
        name: 'updateTimerById'
    }, function(req, res, next) {
        tqs.updateTimer(req.params.timerid, req.body, function(err, timerObject) {
            if (err) {
                res.send(500, err);
            } else if (timerObject) {
                res.send(timerObject);
            } else {
                res.send(404, "ENTRY NOT FOUND");
            }
            next();
        });
    });

    server.del({
        path: '/tqs/timer-mgmt/timers/:timerid',
        name: 'deleteTimerById'
    }, function(req, res, next) {
        tqs.deleteTimer(req.params.timerid, function(err, object) {
            if (err) {
                res.send(500, err);
            } else if (object) {
                res.send(object);
            } else {
                res.send(404, "ENTRY NOT FOUND");
            }
            next();
        });
    });


    server.post({
        path: '/tqs/timer-mgmt/test-echo',
        name: 'test-echo'
    }, function(req, res, next) {
        res.send(200, req.body);
        next();
    });

};

//// END REST API ///
