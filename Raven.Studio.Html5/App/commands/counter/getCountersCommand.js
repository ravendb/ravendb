var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/counter/counter", "common/appUrl"], function(require, exports, commandBase, counter, appUrl) {
    var getCountersCommand = (function (_super) {
        __extends(getCountersCommand, _super);
        /**
        * @param ownerDb The database the collections will belong to.
        */
        function getCountersCommand(skip, take, counterGroupName) {
            _super.call(this);
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

            var url = "/counters/test/counters" + this.urlEncodeArgs(args);
            var selector = function (dtos) {
                return dtos.map(function (d) {
                    return new counter(d);
                });
            };
            return this.query(url, null, appUrl.getSystemDatabase(), selector);
        };
        return getCountersCommand;
    })(commandBase);

    
    return getCountersCommand;
});
//# sourceMappingURL=getCountersCommand.js.map
