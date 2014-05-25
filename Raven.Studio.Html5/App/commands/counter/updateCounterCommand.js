var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/counter/counter"], function(require, exports, commandBase, counter) {
    var updateCounterCommand = (function (_super) {
        __extends(updateCounterCommand, _super);
        /**
        * @param ownerDb The database the collections will belong to.
        */
        function updateCounterCommand(storage, editedCounter, delta) {
            _super.call(this);
            this.storage = storage;
            this.editedCounter = editedCounter;
            this.delta = delta;
        }
        updateCounterCommand.prototype.execute = function () {
            var args = {
                counterName: this.editedCounter.id(),
                group: this.editedCounter.group(),
                delta: this.delta
            };

            var url = "/change";
            var selector = function (dtos) {
                return dtos.map(function (d) {
                    return new counter(d);
                });
            };
            return this.query(url, args, this.storage, selector);
        };
        return updateCounterCommand;
    })(commandBase);

    
    return updateCounterCommand;
});
//# sourceMappingURL=updateCounterCommand.js.map
