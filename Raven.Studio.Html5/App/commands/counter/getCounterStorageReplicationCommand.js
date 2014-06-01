var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, commandBase) {
    var getCounterStorageReplicationCommand = (function (_super) {
        __extends(getCounterStorageReplicationCommand, _super);
        function getCounterStorageReplicationCommand(counterStorage, reportRefreshProgress) {
            if (typeof reportRefreshProgress === "undefined") { reportRefreshProgress = false; }
            _super.call(this);
            this.counterStorage = counterStorage;
            this.reportRefreshProgress = reportRefreshProgress;
            if (!counterStorage) {
                throw new Error("Must specify counter storage");
            }
        }
        getCounterStorageReplicationCommand.prototype.execute = function () {
            var _this = this;
            var url = "/replications/get";
            var getTask = this.query(url, null, this.counterStorage);

            if (this.reportRefreshProgress) {
                getTask.done(function () {
                    return _this.reportSuccess("Replication Destionations of '" + _this.counterStorage.name + "' were successfully refreshed!");
                });
                getTask.fail(function (response) {
                    return _this.reportWarning("There are no saved replication destionations on the server!", response.responseText, response.statusText);
                });
            }
            return getTask;
        };
        return getCounterStorageReplicationCommand;
    })(commandBase);

    
    return getCounterStorageReplicationCommand;
});
//# sourceMappingURL=getCounterStorageReplicationCommand.js.map
