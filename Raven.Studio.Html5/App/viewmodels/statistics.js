var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/getDatabaseStatsCommand", "models/database", "moment"], function(require, exports, getDatabaseStatsCommand, database, moment) {
    var statistics = (function (_super) {
        __extends(statistics, _super);
        function statistics() {
            _super.call(this);
            this.stats = ko.observable();
        }
        statistics.prototype.activate = function (args) {
            var _this = this;
            _super.prototype.activate.call(this, args);

            this.activeDatabase.subscribe(function () {
                return _this.fetchStats();
            });
            this.fetchStats();
        };

        statistics.prototype.fetchStats = function () {
            var _this = this;
            var db = this.activeDatabase();
            if (db) {
                return new getDatabaseStatsCommand(db).execute().done(function (result) {
                    return _this.processStatsResults(result);
                });
            }

            return null;
        };

        statistics.prototype.processStatsResults = function (results) {
            var _this = this;
            // Attach some human readable dates to the indexes.
            results.Indexes.forEach(function (i) {
                i['CreatedTimestampText'] = _this.createHumanReadableTimeDuration(i.CreatedTimestamp);
                i['LastIndexedTimestampText'] = _this.createHumanReadableTimeDuration(i.LastIndexedTimestamp);
                i['LastQueryTimestampText'] = _this.createHumanReadableTimeDuration(i.LastQueryTimestamp);
                i['LastIndexingTimeText'] = _this.createHumanReadableTimeDuration(i.LastIndexingTime);
                i['LastReducedTimestampText'] = _this.createHumanReadableTimeDuration(i.LastReducedTimestamp);
            });

            this.stats(results);
        };

        statistics.prototype.createHumanReadableTimeDuration = function (aspnetJsonDate) {
            if (aspnetJsonDate) {
                var dateMoment = moment(aspnetJsonDate);
                var now = moment();
                var agoInMs = dateMoment.diff(now);
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MMMM Do YYYY, h:mma)");
            }

            return aspnetJsonDate;
        };
        return statistics;
    })(activeDbViewModelBase);

    
    return statistics;
});
//# sourceMappingURL=statistics.js.map
