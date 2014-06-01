var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/counter/counter"], function(require, exports, commandBase, counter) {
    var getCountersCommand = (function (_super) {
        __extends(getCountersCommand, _super);
        /**
        * @param counterStorage - the counter storage that is being used
        * @param skip - number of entries to skip
        * @param take - number of entries to take
        * @param counterGroupName - the counter group to take the entries from
        */
        function getCountersCommand(storage, skip, take, counterGroupName) {
            _super.call(this);
            this.storage = storage;
            this.skip = skip;
            this.take = take;
            this.counterGroupName = counterGroupName;
        }
        getCountersCommand.prototype.execute = function () {
            var args = {
                skip: this.skip,
                take: this.take,
                counterGroupName: this.counterGroupName
            };

            var url = "/counters";
            var selector = function (dtos) {
                return dtos.map(function (d) {
                    return new counter(d);
                });
            };
            return this.query(url, args, this.storage, selector);
        };
        return getCountersCommand;
    })(commandBase);

    
    return getCountersCommand;
});
//# sourceMappingURL=getCountersCommand.js.map
