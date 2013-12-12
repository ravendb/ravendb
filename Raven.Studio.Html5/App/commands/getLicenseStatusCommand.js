var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase", "models/database"], function(require, exports, __commandBase__, __database__) {
    var commandBase = __commandBase__;
    var database = __database__;

    var getLicenseStatusCommand = (function (_super) {
        __extends(getLicenseStatusCommand, _super);
        function getLicenseStatusCommand() {
            _super.apply(this, arguments);
        }
        getLicenseStatusCommand.prototype.execute = function () {
            return this.query("/license/status", null);
        };
        return getLicenseStatusCommand;
    })(commandBase);

    
    return getLicenseStatusCommand;
});
//# sourceMappingURL=getLicenseStatusCommand.js.map
