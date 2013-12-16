import durandalRouter = require("plugins/router");
import raven = require("common/raven");
import database = require("models/database");
import appUrl = require("common/appUrl");

class settings {

    router = null;
    activeDatabase = ko.observable<database>().subscribeTo("ActivateDatabase");
    isOnSystemDatabase: KnockoutComputed<boolean>;
    isOnUserDatabase: KnockoutComputed<boolean>;

    constructor() {

        this.isOnSystemDatabase = ko.computed(() => this.activeDatabase() && this.activeDatabase().isSystem);
        this.isOnUserDatabase = ko.computed(() => this.activeDatabase() && !this.isOnSystemDatabase());

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'settings/apiKeys', moduleId: 'viewModels/apiKeys', title: 'API Keys', type: 'intro', nav: true },
                { route: 'settings/windowsAuth', moduleId: 'viewModels/windowsAuth', title: 'Windows Authentication', type: 'intro', nav: true },
                { route: 'settings/databaseSettings', moduleId: 'viewModels/databaseSettings', title: 'Database Settings', type: 'intro', nav: true },
                { route: 'settings/periodicBackup', moduleId: 'viewModels/periodicBackup', title: 'Periodic Backup', type: 'intro', nav: true }
            ])
            .buildNavigationModel();
    }

    activate(args) {
        this.activeDatabase(appUrl.getDatabase());

        if (this.activeDatabase()) {
            console.log("ZZZ", this.activeDatabase().isSystem);
            if (this.activeDatabase().isSystem) {
                this.router.navigate("settings/apiKeys");
            } else {
                this.router.navigate("settings/databaseSettings");
            }
        }
    }

    routeIsVisible(route: DurandalRouteConfiguration) {
        if (route.title === "Periodic Backup" || route.title === "Database Settings") {
            // Periodic backup and database settings are visible only when we're on a user database.
            return this.isOnUserDatabase();
        } else {
            // API keys and Windows Auth are visible only when we're on the system database.
            return this.isOnSystemDatabase();
        }
    }
}

export = settings;