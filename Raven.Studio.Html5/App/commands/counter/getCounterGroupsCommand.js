var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/counter/counterGroup"], function(require, exports, commandBase, counterGroup) {
    var getCounterGroupsCommand = (function (_super) {
        __extends(getCounterGroupsCommand, _super);
        /**
        * @param ownerDb The database the collections will belong to.
        */
        function getCounterGroupsCommand(storage) {
            _super.call(this);
            this.storage = storage;
        }
        getCounterGroupsCommand.prototype.execute = function () {
            var selector = function (groups) {
                return groups.map(function (g) {
                    return new counterGroup(g);
                });
            };
            return this.query("/groups", null, this.storage, selector);
        };
        return getCounterGroupsCommand;
    })(commandBase);

    
    return getCounterGroupsCommand;
});
//# sourceMappingURL=getCounterGroupsCommand.js.map
