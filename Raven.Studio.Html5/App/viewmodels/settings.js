define(["require", "exports", "plugins/router", "common/raven", "models/database", "common/appUrl"], function(require, exports, durandalRouter, raven, database, appUrl) {
    var settings = (function () {
        function settings() {
            var _this = this;
            this.router = null;
            this.activeDatabase = ko.observable().subscribeTo("ActivateDatabase");
            this.isOnSystemDatabase = ko.computed(function () {
                return _this.activeDatabase() && _this.activeDatabase().isSystem;
            });
            this.isOnUserDatabase = ko.computed(function () {
                return _this.activeDatabase() && !_this.isOnSystemDatabase();
            });

            this.router = durandalRouter.createChildRouter().map([
                { route: 'settings/apiKeys', moduleId: 'viewModels/apiKeys', title: 'API Keys', type: 'intro', nav: true },
                { route: 'settings/windowsAuth', moduleId: 'viewModels/windowsAuth', title: 'Windows Authentication', type: 'intro', nav: true },
                { route: 'settings/databaseSettings', moduleId: 'viewModels/databaseSettings', title: 'Database Settings', type: 'intro', nav: true },
                { route: 'settings/periodicBackup', moduleId: 'viewModels/periodicBackup', title: 'Periodic Backup', type: 'intro', nav: true }
            ]).buildNavigationModel();
        }
        settings.prototype.activate = function (args) {
            this.activeDatabase(appUrl.getDatabase());

            if (this.activeDatabase()) {
                if (this.activeDatabase().isSystem) {
                    this.router.navigate("settings/apiKeys");
                } else {
                    this.router.navigate("settings/databaseSettings");
                }
            }
        };

        settings.prototype.routeIsVisible = function (route) {
            if (route.title === "Periodic Backup" || route.title === "Database Settings") {
                // Periodic backup and database settings are visible only when we're on a user database.
                return this.isOnUserDatabase();
            } else {
                // API keys and Windows Auth are visible only when we're on the system database.
                return this.isOnSystemDatabase();
            }
        };
        return settings;
    })();

    
    return settings;
});
//# sourceMappingURL=settings.js.map
