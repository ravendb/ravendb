var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, commandBase) {
    var saveCounterStorageReplicationCommand = (function (_super) {
        __extends(saveCounterStorageReplicationCommand, _super);
        function saveCounterStorageReplicationCommand(dto, counterStorage) {
            _super.call(this);
            this.dto = dto;
            this.counterStorage = counterStorage;
        }
        saveCounterStorageReplicationCommand.prototype.execute = function () {
            var _this = this;
            this.reportInfo("Saving counters replication");

            return this.saveSetup().done(function () {
                return _this.reportSuccess("Saved counters replication");
            }).fail(function (response) {
                return _this.reportError("Failed to save counters replication", response.responseText, response.statusText);
            });
        };

        saveCounterStorageReplicationCommand.prototype.saveSetup = function () {
            var url = "/replications-save";
            var putArgs = JSON.stringify(this.dto);
            return this.post(url, putArgs, this.counterStorage, { dataType: undefined });
        };
        return saveCounterStorageReplicationCommand;
    })(commandBase);

    
    return saveCounterStorageReplicationCommand;
});
//# sourceMappingURL=saveCounterStorageReplicationCommand.js.map
