var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/commandBase"], function(require, exports, __commandBase__) {
    var commandBase = __commandBase__;

    var getBuildVersionCommand = (function (_super) {
        __extends(getBuildVersionCommand, _super);
        function getBuildVersionCommand() {
            _super.apply(this, arguments);
        }
        getBuildVersionCommand.prototype.execute = function () {
            return this.query("/build/version", null);
        };
        return getBuildVersionCommand;
    })(commandBase);

    
    return getBuildVersionCommand;
});
//# sourceMappingURL=getBuildVersionCommand.js.map
