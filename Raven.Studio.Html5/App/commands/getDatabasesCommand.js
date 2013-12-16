var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database"], function(require, exports, __commandBase__, __database__) {
    var commandBase = __commandBase__;
    var database = __database__;

    var getDatabasesCommand = (function (_super) {
        __extends(getDatabasesCommand, _super);
        function getDatabasesCommand() {
            _super.apply(this, arguments);
        }
        getDatabasesCommand.prototype.execute = function () {
            var resultsSelector = function (databaseNames) {
                return databaseNames.map(function (n) {
                    return new database(n);
                });
            };
            return this.query("/databases", { pageSize: 1024 }, null, resultsSelector);
        };
        return getDatabasesCommand;
    })(commandBase);

    
    return getDatabasesCommand;
});
//# sourceMappingURL=getDatabasesCommand.js.map
