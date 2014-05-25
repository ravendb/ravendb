var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, commandBase) {
    var getCounterStorageReplicationCommand = (function (_super) {
        __extends(getCounterStorageReplicationCommand, _super);
        function getCounterStorageReplicationCommand(counterStorage) {
            _super.call(this);
            this.counterStorage = counterStorage;
            if (!counterStorage) {
                throw new Error("Must specify counter storage");
            }
        }
        getCounterStorageReplicationCommand.prototype.execute = function () {
            var url = "/replications-get";
            return this.query(url, null, this.counterStorage);
        };
        return getCounterStorageReplicationCommand;
    })(commandBase);

    
    return getCounterStorageReplicationCommand;
});
//# sourceMappingURL=getCounterStorageReplicationCommand.js.map
