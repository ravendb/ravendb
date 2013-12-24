var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "durandal/app", "commands/getLogsCommand", "viewmodels/activeDbViewModelBase", "commands/getDatabaseStatsCommand", "models/database", "moment", "viewmodels/copyDocuments", "models/document"], function(require, exports, app, getLogsCommand, activeDbViewModelBase, getDatabaseStatsCommand, database, moment, copyDocuments, document) {
    var logs = (function (_super) {
        __extends(logs, _super);
        function logs() {
            var _this = this;
            _super.call(this);
            this.fetchedLogs = ko.observableArray();
            this.filterLevel = ko.observable("All");
            this.selectedLog = ko.observable();
            this.searchText = ko.observable("");

            this.debugLogCount = ko.computed(function () {
                return _this.fetchedLogs().count(function (l) {
                    return l.Level === "Debug";
                });
            });
            this.infoLogCount = ko.computed(function () {
                return _this.fetchedLogs().count(function (l) {
                    return l.Level === "Info";
                });
            });
            this.warningLogCount = ko.computed(function () {
                return _this.fetchedLogs().count(function (l) {
                    return l.Level === "Warn";
                });
            });
            this.errorLogCount = ko.computed(function () {
                return _this.fetchedLogs().count(function (l) {
                    return l.Level === "Error";
                });
            });
            this.fatalLogCount = ko.computed(function () {
                return _this.fetchedLogs().count(function (l) {
                    return l.Level === "Fatal";
                });
            });
            this.searchTextThrottled = this.searchText.throttle(200);
        }
        logs.prototype.activate = function (args) {
            _super.prototype.activate.call(this, args);
            return this.fetchLogs();
        };

        logs.prototype.fetchLogs = function () {
            var _this = this;
            var db = this.activeDatabase();
            if (db) {
                return new getLogsCommand(db).execute().done(function (results) {
                    return _this.processLogResults(results);
                });
            }

            return null;
        };

        logs.prototype.processLogResults = function (results) {
            var _this = this;
            var now = moment();
            results.forEach(function (r) {
                r['TimeStampText'] = _this.createHumanReadableTime(r.TimeStamp, now);
                r['IsVisible'] = ko.computed(function () {
                    return _this.matchesFilterAndSearch(r);
                });
            });
            this.fetchedLogs(results.reverse());
        };

        logs.prototype.matchesFilterAndSearch = function (log) {
            var searchTextThrottled = this.searchTextThrottled().toLowerCase();
            var filterLevel = this.filterLevel();
            var matchesLogLevel = filterLevel === "All" || log.Level === filterLevel;
            var matchesSearchText = !searchTextThrottled || (log.Message && log.Message.toLowerCase().indexOf(searchTextThrottled) >= 0) || (log.Exception && log.Exception.toLowerCase().indexOf(searchTextThrottled) >= 0);

            return matchesLogLevel && matchesSearchText;
        };

        logs.prototype.createHumanReadableTime = function (time, now) {
            if (time) {
                var dateMoment = moment(time);
                var agoInMs = dateMoment.diff(now);
                return moment.duration(agoInMs).humanize(true) + dateMoment.format(" (MM/DD/YY, h:mma)");
            }

            return time;
        };

        logs.prototype.selectLog = function (log) {
            this.selectedLog(log);
        };

        logs.prototype.tableKeyDown = function (sender, e) {
            var isKeyUp = e.keyCode === 38;
            var isKeyDown = e.keyCode === 40;
            if (isKeyUp || isKeyDown) {
                e.preventDefault();

                var oldSelection = this.selectedLog();
                if (oldSelection) {
                    var oldSelectionIndex = this.fetchedLogs.indexOf(oldSelection);
                    var newSelectionIndex = oldSelectionIndex;
                    if (isKeyUp && oldSelectionIndex > 0) {
                        newSelectionIndex--;
                    } else if (isKeyDown && oldSelectionIndex < this.fetchedLogs().length - 1) {
                        newSelectionIndex++;
                    }

                    this.selectedLog(this.fetchedLogs()[newSelectionIndex]);
                    var newSelectedRow = $("#logsContainer table tbody tr:nth-child(" + (newSelectionIndex + 1) + ")");
                    if (newSelectedRow) {
                        this.ensureRowVisible(newSelectedRow);
                    }
                }
            }
        };

        logs.prototype.ensureRowVisible = function (row) {
            var table = $("#logTableContainer");
            var scrollTop = table.scrollTop();
            var scrollBottom = scrollTop + table.height();
            var scrollHeight = scrollBottom - scrollTop;

            var rowPosition = row.position();
            var rowTop = rowPosition.top;
            var rowBottom = rowTop + row.height();

            if (rowTop < 0) {
                table.scrollTop(scrollTop + rowTop);
            } else if (rowBottom > scrollHeight) {
                table.scrollTop(scrollTop + (rowBottom - scrollHeight));
            }
        };

        logs.prototype.setFilterAll = function () {
            this.filterLevel("All");
        };

        logs.prototype.setFilterDebug = function () {
            this.filterLevel("Debug");
        };

        logs.prototype.setFilterInfo = function () {
            this.filterLevel("Info");
        };

        logs.prototype.setFilterWarning = function () {
            this.filterLevel("Warn");
        };

        logs.prototype.setFilterError = function () {
            this.filterLevel("Error");
        };

        logs.prototype.setFilterFatal = function () {
            this.filterLevel("Fatal");
        };
        return logs;
    })(activeDbViewModelBase);

    
    return logs;
});
//# sourceMappingURL=logs.js.map
