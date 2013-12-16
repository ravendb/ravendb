var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database"], function(require, exports, __commandBase__, __database__) {
    var commandBase = __commandBase__;
    var database = __database__;

    var getDatabaseStatsCommand = (function (_super) {
        __extends(getDatabaseStatsCommand, _super);
        function getDatabaseStatsCommand(db) {
            _super.call(this);
            this.db = db;
        }
        getDatabaseStatsCommand.prototype.execute = function () {
            var url = this.db.isSystem ? "/stats" : "/databases/" + this.db.name + "/stats";
            return this.query(url, null, null);
        };
        return getDatabaseStatsCommand;
    })(commandBase);

    
    return getDatabaseStatsCommand;
});
//# sourceMappingURL=getDatabaseStatsCommand.js.map
