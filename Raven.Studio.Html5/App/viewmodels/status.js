define(["require", "exports", "plugins/router"], function(require, exports, durandalRouter) {
    var status = (function () {
        function status() {
            this.displayName = "status";
            this.router = null;
            this.router = durandalRouter.createChildRouter().map([
                { route: 'status', moduleId: 'viewmodels/statistics', title: 'Stats', type: 'intro', nav: false },
                { route: 'status/statistics', moduleId: 'viewmodels/statistics', title: 'Stats', type: 'intro', nav: true },
                { route: 'status/logs', moduleId: 'viewmodels/logs', title: 'Logs', nav: true },
                { route: 'status/alerts', moduleId: 'viewmodels/alerts', title: 'Alerts', nav: true },
                { route: 'status/indexErrors', moduleId: 'viewmodels/indexErrors', title: 'Index Errors', nav: true },
                { route: 'status/replicationStats', moduleId: 'viewmodels/replicationStats', title: 'Replication Stats', nav: true },
                { route: 'status/userInfo', moduleId: 'viewmodels/userInfo', title: 'User Info', type: 'intro', nav: true }
            ]).buildNavigationModel();
        }
        status.prototype.activate = function (args) {
            //this.router.navigate("status/statistics");
        };

        status.prototype.canDeactivate = function () {
            return true;
        };
        return status;
    })();

    
    return status;
});
//# sourceMappingURL=status.js.map
