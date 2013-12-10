define(["require", "exports", "plugins/router", "common/raven", "models/database"], function(require, exports, durandalRouter, raven, database) {
    var settings = (function () {
        function settings() {
            var _this = this;
            this.router = null;
            this.activeDatabase = raven.activeDatabase;
            this.isOnSystemDatabase = ko.computed(function () {
                return _this.activeDatabase() && _this.activeDatabase().isSystem;
            });
            this.isOnUserDatabase = ko.computed(function () {
                return _this.activeDatabase() && !_this.isOnSystemDatabase();
            });

            this.router = durandalRouter.createChildRouter().map([
                { route: 'settings/apiKeys', moduleId: 'viewModels/apiKeys', title: 'API Keys', type: 'intro', nav: true },
                { route: 'settings/windowsAuth', moduleId: 'viewModels/windowsAuth', title: 'Windows Authentication', type: 'intro', nav: this.isOnSystemDatabase },
                { route: 'settings/databaseSettings', moduleId: 'viewModels/databaseSettings', title: 'Database Settings', type: 'intro', nav: this.isOnUserDatabase },
                { route: 'settings/periodicBackup', moduleId: 'viewModels/periodicBackup', title: 'Periodic Backup', type: 'intro', nav: this.isOnUserDatabase }
            ]).buildNavigationModel();
        }
        settings.prototype.activate = function (args) {
            console.log("zzzactivating settings!", args);
            if (this.activeDatabase()) {
                if (this.activeDatabase().isSystem) {
                    console.log("zzzznav to api keys");
                    this.router.navigate("settings/apiKeys");
                } else {
                    console.log("zzzznav to db settings");
                    this.router.navigate("settings/databaseSettings");
                }
            }
        };
        return settings;
    })();

    
    return settings;
});
//# sourceMappingURL=settings.js.map
