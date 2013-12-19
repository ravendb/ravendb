var __extends = this.__extends || function (d, b) {
    for (var p in b) if (b.hasOwnProperty(p)) d[p] = b[p];
    function __() { this.constructor = d; }
    __.prototype = b.prototype;
    d.prototype = new __();
};
define(["require", "exports", "plugins/router", "models/database", "viewmodels/activeDbViewModelBase"], function(require, exports, durandalRouter, database, activeDbViewModelBase) {
    var status = (function (_super) {
        __extends(status, _super);
        function status() {
            _super.call(this);

            this.router = durandalRouter.createChildRouter().map([
                { route: 'status', moduleId: 'viewmodels/statistics', title: 'Stats', nav: true },
                { route: 'status/logs', moduleId: 'viewmodels/logs', title: 'Logs', nav: true },
                { route: 'status/alerts', moduleId: 'viewmodels/alerts', title: 'Alerts', nav: true },
                { route: 'status/indexErrors', moduleId: 'viewmodels/indexErrors', title: 'Index Errors', nav: true },
                { route: 'status/replicationStats', moduleId: 'viewmodels/replicationStats', title: 'Replication Stats', nav: true },
                { route: 'status/userInfo', moduleId: 'viewmodels/userInfo', title: 'User Info', nav: true }
            ]).buildNavigationModel();
        }
        return status;
    })(activeDbViewModelBase);

    
    return status;
});
//# sourceMappingURL=status.js.map
