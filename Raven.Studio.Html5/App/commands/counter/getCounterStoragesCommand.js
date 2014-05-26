var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "models/counter/counterStorage", "commands/commandBase"], function(require, exports, counterStorage, commandBase) {
    var getCounterStoragesCommand = (function (_super) {
        __extends(getCounterStoragesCommand, _super);
        function getCounterStoragesCommand() {
            _super.apply(this, arguments);
        }
        getCounterStoragesCommand.prototype.execute = function () {
            var resultsSelector = function (counterStorageNames) {
                return counterStorageNames.map(function (n) {
                    return new counterStorage(n);
                });
            };
            return this.query("/counterStorage/conterStorages", { pageSize: 1024 }, null, resultsSelector);
        };
        return getCounterStoragesCommand;
    })(commandBase);

    
    return getCounterStoragesCommand;
});
//# sourceMappingURL=getCounterStoragesCommand.js.map
