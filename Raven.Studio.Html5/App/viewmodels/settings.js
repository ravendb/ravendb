define(["require", "exports", "plugins/router", "common/raven"], function(require, exports, __durandalRouter__, __raven__) {
    var durandalRouter = __durandalRouter__;
    var raven = __raven__;

    var settings = (function () {
        function settings() {
            var _this = this;
            this.displayName = "status";
            this.router = null;
            this.activeDatabase = raven.activeDatabase;
            this.isOnSystemDatabase = ko.computed(function () {
                return _this.activeDatabase() && _this.activeDatabase().isSystem;
            });
            this.isOnUserDatabase = ko.computed(function () {
                return _this.activeDatabase() && !_this.isOnSystemDatabase();
            });

            this.router = durandalRouter.createChildRouter().map([
                { route: 'settings/apiKeys', moduleId: 'viewModels/apiKeys', title: 'API Keys', type: 'intro', nav: this.isOnSystemDatabase },
                { route: 'settings/windowsAuth', moduleId: 'viewModels/windowsAuth', title: 'Windows Authentication', type: 'intro', nav: this.isOnSystemDatabase },
                { route: 'settings/databaseSettings', moduleId: 'viewModels/databaseSettings', title: 'Database Settings', type: 'intro', nav: this.isOnUserDatabase },
                { route: 'settings/periodicBackup', moduleId: 'viewModels/periodicBackup', title: 'Periodic Backup', type: 'intro', nav: this.isOnUserDatabase }
            ]).buildNavigationModel();
        }
        settings.prototype.activate = function (args) {
            //if (this.activeDatabase()) {
            //    if (this.activeDatabase().isSystem) {
            //        this.router.navigate("settings/apiKeys");
            //    } else {
            //        this.router.navigate("settings/databaseSettings");
            //    }
            //}
        };
        return settings;
    })();

    
    return settings;
});
//# sourceMappingURL=settings.js.map
