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
                { route: 'settings/apiKeys', moduleId: 'viewmodels/apiKeys', title: 'API Keys', nav: true },
                { route: 'settings/windowsAuth', moduleId: 'viewmodels/windowsAuth', title: 'Windows Authentication', nav: true },
                { route: 'settings/databaseSettings', moduleId: 'viewmodels/databaseSettings', title: 'Database Settings', nav: true },
                { route: 'settings/periodicBackup', moduleId: 'viewmodels/periodicBackup', title: 'Periodic Backup', nav: true }
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
