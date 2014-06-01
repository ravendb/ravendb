var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/counter/counter"], function(require, exports, commandBase, counter) {
    var getCounterOverallTotalCommand = (function (_super) {
        __extends(getCounterOverallTotalCommand, _super);
        /**
        * @param ownerDb The database the collections will belong to.
        */
        function getCounterOverallTotalCommand(storage, counterToReceive) {
            _super.call(this);
            this.storage = storage;
            this.counterToReceive = counterToReceive;
        }
        getCounterOverallTotalCommand.prototype.execute = function () {
            var args = {
                group: this.counterToReceive.group(),
                counterName: this.counterToReceive.id()
            };

            var url = "/getCounterOverallTotal";
            var selector = function (dto) {
                return new counter(dto);
            };
            return this.query(url, args, this.storage, selector);
        };
        return getCounterOverallTotalCommand;
    })(commandBase);

    
    return getCounterOverallTotalCommand;
});
//# sourceMappingURL=getCounterValueCommand.js.map
