var util = require('util');
var uuid = require('uuid');
var mongoose = require('mongoose');
var request = require('request');
var later = require('later');
var bunyan = require('bunyan')
var templateDb = GLOBAL.templateDb;
var evoDb = GLOBAL.evoDb;
var CONFIG;
GLOBAL.tqslib = exports;

var intervalObjects = {};
var timerSchedules = {};
var timeoutObjects = {};

var tqsSchema = mongoose.Schema({
    timerId: {
        type: String,
        unique: true,
        index: true
    },
	timerType: {
        type: String,
        lowercase: true,
        enum: [
            'cronexpression',
            'once',
            'textexpression'
        ]
    },
    sourceReference: {
        type: String,
        index: true
    },
	timerStartTime: {
        type: Date,
        index: true,
        default: Date.now
    },
	timerEndTime: {
        type: Date,
        index: true
    },
	schedule: String,
    hasSecondResolution: Boolean,
	callbackurl: {
        type: String,
        required: true
    },
    method: {
        type: String,
        default: 'POST',
        enum: [ 'GET', 'POST', 'PUT', 'DELETE' ]
    },
    data: mongoose.Schema.Types.Mixed,
	createDateTime: {
        type: Date,
        default: Date.now
    },
	isCompleted: {
        type: Boolean,
        index: true,
        default: false
    },
	completedDateTime: {
        type: Date
    },
    hasError: {
        type: Boolean,
        default: false
    }
});

var tqsAuditSchema = mongoose.Schema({
    timerId: {
        type: String,
        index: true
    },
    auditType: {
        type: String,
        enum: [
            'created',
            'scheduled',
            'updated',
            'skipped',
            'executed',
            'completed'
        ]
    },
    status: {
        type: String,
        enum: [
            'success',
            'fail'
        ]
    },
    eventdatetime: {
        type: Date,
        default: Date.now
    },
    message: String,
    response: mongoose.Schema.Types.Mixed,
    error: mongoose.Schema.Types.Mixed,
    data: mongoose.Schema.Types.Mixed
});

var tqs_codes = {
	failure: "tqs-error",
	success: "tqs-success"
};

exports.tqs_codes = tqs_codes;

var tqsModel, tqsAudit;

exports.init = function(config, loggerinstance) {
	CONFIG = config;
    logger = loggerinstance;

	CONFIG.restproxBaseURL = "http://localhost:9001" //CONFIG.restproxBaseURL || "http://localhost";
	var tqsHost = (config.tqsDb && config.tqsDb.host) || 'localhost';
	var tqsName = (config.tqsDb && config.tqsDb.database) || 'tqs';
	GLOBAL.tqsDb = mongoose.createConnection('mongodb://' + tqsHost + '/' + tqsName);
	GLOBAL.tqsDb.on('error', function(err) {
		logger.error({
			'monogo_host': tqsHost,
			'mongo_db': tqsName,
            'error': err
		}, "MONGO EXCEPTION");
	});
	tqsModel = GLOBAL.tqsDb.model('tqs', tqsSchema);
    tqsAudit = GLOBAL.tqsDb.model('audit', tqsAuditSchema);
	logger.info("TQS Service initialized");
    coldStart();
};

exports.publishTimer = function(timerData, callback) {
	if (!timerData.timerId) timerData.timerId = uuid.v4();
	logger.info(timerData, "publishTimer initiated with timer object");
	var tqsmodel = new tqsModel(timerData);
    var audit = new tqsAudit ({
        timerId: timerData.timerId,
        auditType: "created"
    });
	tqsmodel.save(function(err, object) {
        if (err) {
            audit.status = "fail";
            audit.error = err;
            logger.error(err, "FAILED TO SAVE NEW TIMER");
        } else {
            audit.status = "success";
            audit.data = timerData;
            err = createTimerSchedule (object);
        }
        audit.save();
        callback(err, object);
	});
};

exports.searchTimers = function(query, callback) {
    tqsModel.find(query, callback);
};

exports.updateTimer = function(timerId, timerData, callback) {
	logger.info(timerData, "updateTimer request initiated with timer object");
    var audit = new tqsAudit ({
        timerId: timerData.timerId,
        auditType: "updated",
        data: timerData
    });
	tqsModel.findOne({
		'timerId': timerId
	}, function(err, tqsmodel) {

		if (tqsmodel) {
			for (var attr in timerData) {
				tqsmodel[attr] = timerData[attr];
			}

            var rescheduled = (timerData.schedule && timerData.schedule !== tqsmodel.schedule) ||
                          (timerData.timerStartTime && timerData.timerSchedules !== tqsmodel.timerStartTime) ||
                          (timerData.timerEndTime && timerData.timerEndTime !== tqsmodel.timerEndTime);

            var cancelled = timerData.isCompleted &&
                        (timerData.isCompleted === true || timerData.isCompleted == 'true');

            if (rescheduled) {
                clearTimer(timerId);
                createTimerSchedule(tqsmodel);
                if (timerData.timerEndTime) {
                    // edge case: update a timer, but new schedule has no instances before the endtime.
                    var mySchedule = timerSchedules[timerId];
                    var nextOccurrenceTime = mySchedule ? later.schedule(mySchedule).next() : null;
                    if (nextOccurrenceTime && nextOccurrenceTime > new Date(timerData.timerEndTime)) {
                        cancelled = true;
                    }
                }
            }
            if (cancelled) {
                clearTimer(timerId);
                tqsmodel.isCompleted = true;
            }

			tqsmodel.save(function(err, object) {
				if (err) {
					console.log("Error updateTimer: " + err);
                    logger.error(err, "exception in updateTimer");
                    audit.status = "fail";
                    audit.error = err;
                } else {
                    audit.status = "success";
                }
                audit.save();
                callback(err, object);
			});
		} else {
			logger.warn("updateTimer: unable to find timer in mongo for given timer id - should never happen");
			logger.error(err, "exception in updateTimer");
            callback(new Error("unable to find timer"));
		}
	});
};

exports.findTimerById = function(timerId, callback) {
	tqsModel.findOne({
		'timerId': timerId
	}, function(err, mapObject) {
		if (err) {
			logger.error(err, "exception in findTimerById");
		};
		callback(err, mapObject);
	});
};

exports.getTimerAuditsByTimerId = function(timerId, q, callback) {
    var query = tqsAudit.find({ 'timerId': timerId })
        .sort({'eventdatetime': -1});

    if (q.limit) query = query.limit(q.limit);
    if (q.skip) query = query.skip(q.skip);

    query.exec().then ( function(timerAudits) {
        callback(null, timerAudits);
    }, function (err) {
        logger.error(err, "exception in getTimerAuditsByTimerId");
        callback(err);
    });
};

exports.deleteTimer = function(id, callback) {
    var audit = new tqsAudit ({
        timerId: id,
        auditType: "completed",
        data : { timerId: id },
        message : "timer was deleted"
    });
	tqsModel.findOne({
		'timerId': id
	}, function(err, timerObject) {
		if (err) {
            audit.error = err;
            audit.status = 'fail';
            audit.save();
			callback(err);
			logger.error(err, "exception in deleteTimer");
		} else if (!timerObject) {
            audit.error = new Error("Timer for UUID:" + id + " NOT FOUND");
            audit.status = 'fail';
            callback('timer not found');
        } else {
            timerObject.remove(function(err) {
                if (err) {
                    audit.error = err;
                    audit.status = 'fail';
                } else {
                    audit.status = 'success';
                    audit.data = timerObject;
                }
                if ( !timerObject.isCompleted ) {
                    if ( intervalObjects[id] ) {
                        audit.message = "Timer Deleted: Recurring Schedule Cleared";
                        clearTimer(id);
                    } else if ( timeoutObjects[id] ) {
                        audit.message = "Timer Deleted: Schedule (once) Cleared";
                        clearTimer(id);
                    } else {
                        audit.message = "Timer deleted but no schedule found (and not completed)";
                        logger.warn(audit.message);
                    }
                }
                audit.save(function (err, obj) {
                    if (err) {
                        logger.error ("FAIL");
                    }
                });
                callback(err, timerObject);
			});
        }
	});
};

// TODO: in a multi-instance configuration, we need election strategy for allocating jobs
var coldStart = function() {
    logger.info("TQS cold start");
    tqsModel.find( {
        'isCompleted': false
    }, function(err, timers) {
        logger.info("TQS number of timers found: " + timers.length);
        if (err) {
            logger.error(err, "exception in getTimers");
        } else if (!timers) {
            logger.info("TQS start-up found no active timers");
        } else {
            timers.forEach(function (timer) {
                createTimerSchedule(timer);
            });
        }
    });
}

function createTimerSchedule(timer) {
    var timerType = timer.timerType;
    var timerSchedule, timeoutOffset, startTime, endTime, now;

    logger.debug("TQS-createTimerSchedule", "Creating schedule for timer: %s", util.inspect(timer));

    var audit = new tqsAudit ({
        timerId: timer.timerId,
        auditType: "scheduled"
    });

    if (timerType === 'once') {
        var timeoutOffset = new Date(timer.timerStartTime) - new Date();
        if (timeoutOffset > 0) {
            // occurrence is in the future, schedule it
            timeoutObjects[timer.timerId] =
                setTimeout(function() {
                    executeTimer(timer);
                }, timeoutOffset);

        } else {
            // we missed the timer??? what should we do??
            // TODO: system was down and we missed the timer
            executeTimer(timer);
        }
        audit.message = "ONCE timer scheduled for " + timer.timerStartTime;
        timer.scheduleCreated = true;

    } else if (timerType === 'textexpression' || timerType === 'cronexpression') {
        // Set later time calculation absed on UTC time as
        // we expect start and end date to be passed as UTC from server
        later.date.UTC();

        var parseSchedule = function () {
            if (timerType === 'textexpression') {
                timerSchedule = later.parse.text(timer.schedule, timer.hasSecondResolution || false);

            } else if (timerType === 'cronexpression') {
                timerSchedule = later.parse.cron(timer.schedule, timer.hasSecondResolution || false);

            } else {
                timerSchedule = { error : 1 };
                audit.message = "Invalid timerType: " + timerType;
            }
        }

        try {
            parseSchedule();

        } catch (err) {

            timerSchedule = { error: 1 }
            audit.message = "Exception parsing schedule: " + timer.schedule;
            audit.error = err;
        }

        if (timerSchedule.error > 0) {
            logger.error(timerSchedule, "FAIL TO PARSE RECURRING TEXT SCHEDULE");
            audit.error = timerSchedule.error;
            if (!audit.message) { audit.message = "Schedule parser error on character number " + timerSchedule.error }
            timer.isCompleted = true;
            timer.completedDateTime = new Date();
            timer.hasError = true;
            timer.save();
            audit.save();
            return new Error('422: ' + audit.message);

        } else {
            audit.message = "created a recurring text schedule for " + timer.schedule;
            audit.data = timerSchedule;
            audit.status = 'success';

            timerSchedules[timer.timerId] = timerSchedule;

            try {
                intervalObjects[timer.timerId] =
                    later.setInterval(function() {
                            executeTimer(timer);
                        }, timerSchedule);
            } catch (err) {
                timer.isCompleted = true;
                timer.completedDateTime = new Date();
                timer.hasError = true;
                audit.message = "Exception in scheduling: " + timer.schedule;
                audit.error = err;
                timer.save();
                audit.save();
                return new Error('422: ' + audit.message);
            }
        }
    }
    audit.save();
}

function executeTimer(timer) {
    logger.info(timer, "########## Executing Timer #############");
    var err, obj = {};
    var uri = CONFIG.restproxBaseURL + timer.callbackurl;

    var audit = new tqsAudit ({
        timerId: timer.timerId,
        auditType: "executed"
    });

    var currentTime = new Date();

    logger.info(timer.method + " " + uri);

    if (timer.timerStartTime.getTime() <= currentTime.getTime() + 100) {
        request({
            uri: uri,
            method: timer.method || 'POST',
            headers: {
                'content-type': 'application/json',
                'Cookie': 'backdoor=SpiffyFoo' //TODO: remove back door, security context is taken when created
            },
            body: JSON.stringify(timer.data)
        }, function(error, res, body) {
            if (error) {
                console.log('*************TQS execute timer callbackurl request:\n' + error);
                logger.error(error, "exception in executeTimer callbackurl rest call");
                obj.status = tqs_codes.failure;
                audit.status = "fail";
                audit.error = error;
                audit.save();
                return;
            }

            audit.message = "TQS RESULT statusCode: " + res.statusCode + " uri: " + uri;

            try {
                audit.response = JSON.parse(body);
            } catch (err) {
                // failed to parse JSON, save string
                audit.response = body;
            };

            logger.info(body, "TQS RESULT statusCode: " + res.statusCode + " uri: " + uri);

            if (res.statusCode !== 200) { // any other status is a fail
                logger.info("failed request");
                error = new Error ("TQS: " + uri + " STATUS:" + res.status + " RESPONSE: " + body);
                obj.status = tqs_codes.failure;
                audit.status = "fail";
                audit.error = error;
                logger.info("save audit");
                audit.save(function (err) {
                    if (err) {
                        logger.error(err, "FAILED TO Save audit");
                    }
                });
                return;
            }

            obj.status = tqs_codes.success;
            obj.body = body;

            audit.status = "success";
            audit.save();

            logger.info("TQS", "executeTimer callbackurl REST call is DONE uri:%s method:%s object:%s", uri, 'POST', util.inspect(timer));

            var mySchedule = timerSchedules[timer.timerId];
            var nextOccurrenceTime = mySchedule ? later.schedule(mySchedule).next() : null;

            //Update processes timer in memory and mongo
            if (timer.timerType == 'once' || (timer.timerEndTime && nextOccurrenceTime &&
                                              nextOccurrenceTime > timer.timerEndTime)) {
                // timer is completed iff it is a single instance timer (once) or the next occurrence is
                // past the end datetime
                timer.isCompleted = true;
                timer.completedDateTime = new Date();
                clearTimer(timer.timerId);
                timer.save();
            }

        });

    } else {
        audit.auditType = "skipped";
        audit.message = "execution delayed because start time: " + timer.timerStartTime + " : is in the future";
        audit.save();
    }
}

function clearTimer (id) {
    if (timerSchedules[id]) {
        delete timerSchedules[id];
    }
    if (intervalObjects[id]) {
        intervalObjects[id].clear();
        delete intervalObjects[id];
    }
    if (timeoutObjects[id]) {
        clearTimeout(timeoutObjects[id]);
        delete timeoutObjects[id];
    }
}

