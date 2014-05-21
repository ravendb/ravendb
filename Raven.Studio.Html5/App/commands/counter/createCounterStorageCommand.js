var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, commandBase) {
    var createCounterStorageCommand = (function (_super) {
        __extends(createCounterStorageCommand, _super);
        /**
        * @param filesystemName The file system name we are creating.
        */
        function createCounterStorageCommand(counterStorageName, counterStoragePath) {
            _super.call(this);
            this.counterStorageName = counterStorageName;
            this.counterStoragePath = counterStoragePath;

            if (!counterStorageName) {
                this.reportError("Counter Storage must have a name!");
                throw new Error("Counter Storage must have a name!");
            }

            if (this.counterStoragePath == null) {
                this.counterStoragePath = "~\\Counters\\" + this.counterStorageName;
            }
        }
        createCounterStorageCommand.prototype.execute = function () {
            var _this = this;
            this.reportInfo("Creating Counter Storage '" + this.counterStorageName + "'");

            var filesystemDoc = {
                "Settings": { "Raven/Counters/DataDir": this.counterStoragePath },
                "Disabled": false
            };

            var url = "/counterstorage/admin/" + this.counterStorageName;

            var createTask = this.put(url, JSON.stringify(filesystemDoc), null, { dataType: undefined });
            createTask.done(function () {
                return _this.reportSuccess(_this.counterStorageName + " created");
            });
            createTask.fail(function (response) {
                return _this.reportError("Failed to create counter storage", response.responseText, response.statusText);
            });

            return createTask;
        };
        return createCounterStorageCommand;
    })(commandBase);

    
    return createCounterStorageCommand;
});
//# sourceMappingURL=createCounterStorageCommand.js.map
