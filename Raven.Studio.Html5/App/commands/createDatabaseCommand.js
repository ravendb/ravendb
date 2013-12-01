var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, __commandBase__) {
    var commandBase = __commandBase__;

    var createDatabaseCommand = (function (_super) {
        __extends(createDatabaseCommand, _super);
        function createDatabaseCommand(databaseName) {
            _super.call(this);
            this.databaseName = databaseName;
        }
        createDatabaseCommand.prototype.execute = function () {
            var _this = this;
            var createDbTask = this.ravenDb.createDatabase(this.databaseName);

            this.reportInfo("Creating " + this.databaseName);

            createDbTask.done(function () {
                return _this.reportSuccess(_this.databaseName + " created");
            });
            createDbTask.fail(function (response) {
                return _this.reportError("Failed to create database", JSON.stringify(response));
            });
            return createDbTask;
        };
        return createDatabaseCommand;
    })(commandBase);

    
    return createDatabaseCommand;
});
//# sourceMappingURL=createDatabaseCommand.js.map
