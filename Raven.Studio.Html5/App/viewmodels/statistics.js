var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "viewmodels/activeDbViewModelBase", "commands/getDatabaseStatsCommand", "models/database"], function(require, exports, activeDbViewModelBase, getDatabaseStatsCommand, database) {
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
                    return _this.stats(result);
                });
            }

            return null;
        };
        return statistics;
    })(activeDbViewModelBase);

    
    return statistics;
});
//# sourceMappingURL=statistics.js.map
