var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database"], function(require, exports, commandBase, database) {
    var getUserInfoCommand = (function (_super) {
        __extends(getUserInfoCommand, _super);
        function getUserInfoCommand(db) {
            _super.call(this);
            this.db = db;
        }
        getUserInfoCommand.prototype.execute = function () {
            var url = "/debug/user-info";
            return this.query(url, null, this.db);
        };
        return getUserInfoCommand;
    })(commandBase);

    
    return getUserInfoCommand;
});
//# sourceMappingURL=getUserInfoCommand.js.map
