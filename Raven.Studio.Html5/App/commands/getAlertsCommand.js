var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database"], function(require, exports, commandBase, database) {
    var getAlertsCommand = (function (_super) {
        __extends(getAlertsCommand, _super);
        function getAlertsCommand(db) {
            _super.call(this);
            this.db = db;
        }
        getAlertsCommand.prototype.execute = function () {
            return this.query("/docs/Raven/Alerts", null, this.db);
        };
        return getAlertsCommand;
    })(commandBase);

    
    return getAlertsCommand;
});
//# sourceMappingURL=getAlertsCommand.js.map
