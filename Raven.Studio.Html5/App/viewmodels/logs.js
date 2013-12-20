var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "commands/getLogsCommand", "viewmodels/activeDbViewModelBase", "models/database"], function(require, exports, getLogsCommand, activeDbViewModelBase, database) {
    var logs = (function (_super) {
        __extends(logs, _super);
        function logs() {
            _super.apply(this, arguments);
            this.fetchedLogs = ko.observableArray();
        }
        logs.prototype.activate = function (args) {
            var _this = this;
            _super.prototype.activate.call(this, args);

            new getLogsCommand(this.activeDatabase()).execute().done(function (results) {
                return _this.fetchedLogs(results);
            });
        };
        return logs;
    })(activeDbViewModelBase);

    
    return logs;
});
//# sourceMappingURL=logs.js.map
