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

            if (!databaseName) {
                throw new Error("Database must have a name.");
            }
        }
        createDatabaseCommand.prototype.execute = function () {
            var _this = this;
            this.reportInfo("Creating " + this.databaseName);

            // TODO: include selected bundles from UI.
            var databaseDoc = {
                "Settings": {
                    "Raven/DataDir": "~\\Databases\\" + this.databaseName
                },
                "SecuredSettings": {},
                "Disabled": false
            };

            var url = "/admin/databases/" + this.databaseName;
            var createTask = this.put(url, JSON.stringify(databaseDoc), null);

            createTask.done(function () {
                return _this.reportSuccess(_this.databaseName + " created");
            });
            createTask.fail(function (response) {
                return _this.reportError("Failed to create database", JSON.stringify(response));
            });

            // Forces creation of standard indexes? Looks like it.
            createTask.done(function () {
                return _this.query("/databases/" + _this.databaseName + "/silverlight/ensureStartup", null, null);
            });

            return createTask;
        };
        return createDatabaseCommand;
    })(commandBase);

    
    return createDatabaseCommand;
});
//# sourceMappingURL=createDatabaseCommand.js.map
