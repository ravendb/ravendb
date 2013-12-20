var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database"], function(require, exports, commandBase, database) {
    var getLogsCommand = (function (_super) {
        __extends(getLogsCommand, _super);
        function getLogsCommand(db) {
            _super.call(this);
            this.db = db;
        }
        getLogsCommand.prototype.execute = function () {
            var url = "/logs";
            return this.query(url, null, this.db);
        };
        return getLogsCommand;
    })(commandBase);

    
    return getLogsCommand;
});
//# sourceMappingURL=getLogsCommand.js.map
