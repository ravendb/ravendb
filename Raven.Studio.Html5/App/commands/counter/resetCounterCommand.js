var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, commandBase) {
    var resetCounterCommand = (function (_super) {
        __extends(resetCounterCommand, _super);
        /**
        * @param counterStorage - the counter storage that is being used
        * @param editedCounter - the edited counter
        */
        function resetCounterCommand(storage, counterToReset) {
            _super.call(this);
            this.storage = storage;
            this.counterToReset = counterToReset;
        }
        resetCounterCommand.prototype.execute = function () {
            var args = {
                counterName: this.counterToReset.id(),
                group: this.counterToReset.group()
            };

            var url = "/reset" + this.urlEncodeArgs(args);
            return this.post(url, null, this.storage, { dataType: undefined });
        };
        return resetCounterCommand;
    })(commandBase);

    
    return resetCounterCommand;
});
//# sourceMappingURL=resetCounterCommand.js.map
