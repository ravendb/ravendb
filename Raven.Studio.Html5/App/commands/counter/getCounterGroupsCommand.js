var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/counter/counterGroup", "common/appUrl"], function(require, exports, commandBase, counterGroup, appUrl) {
    var getCounterGroupsCommand = (function (_super) {
        __extends(getCounterGroupsCommand, _super);
        /**
        * @param ownerDb The database the collections will belong to.
        */
        function getCounterGroupsCommand() {
            _super.call(this);
        }
        getCounterGroupsCommand.prototype.execute = function () {
            var selector = function (groups) {
                return groups.map(function (g) {
                    return new counterGroup(g);
                });
            };
            return this.query("/counters/test/groups", null, appUrl.getSystemDatabase(), selector);
        };
        return getCounterGroupsCommand;
    })(commandBase);

    
    return getCounterGroupsCommand;
});
//# sourceMappingURL=getCounterGroupsCommand.js.map
