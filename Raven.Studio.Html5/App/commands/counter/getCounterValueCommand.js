var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/counter/counter"], function(require, exports, commandBase, counter) {
    var getCounterValueCommand = (function (_super) {
        __extends(getCounterValueCommand, _super);
        /**
        * @param ownerDb The database the collections will belong to.
        */
        function getCounterValueCommand(storage, counterToReceive) {
            _super.call(this);
            this.storage = storage;
            this.counterToReceive = counterToReceive;
        }
        getCounterValueCommand.prototype.execute = function () {
            var args = {
                counterName: this.counterToReceive.id(),
                group: this.counterToReceive.group()
            };

            var url = "/getCounterValue";
            var selector = function (dto) {
                return new counter(dto);
            };
            return this.query(url, args, this.storage, selector);
        };
        return getCounterValueCommand;
    })(commandBase);

    
    return getCounterValueCommand;
});
//getCounterValue
//# sourceMappingURL=getCounterValueCommand.js.map
