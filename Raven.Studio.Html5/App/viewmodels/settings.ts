import durandalRouter = require("plugins/router");
import raven = require("common/raven");
import database = require("models/database");

class settings {

    router = null;
    activeDatabase = raven.activeDatabase;
    isOnSystemDatabase: KnockoutComputed<boolean>;
    isOnUserDatabase: KnockoutComputed<boolean>;

    constructor() {

        this.isOnSystemDatabase = ko.computed(() => this.activeDatabase() && this.activeDatabase().isSystem);
        this.isOnUserDatabase = ko.computed(() => this.activeDatabase() && !this.isOnSystemDatabase());

        this.router = durandalRouter.createChildRouter()
            .map([
                { route: 'settings/apiKeys', moduleId: 'viewModels/apiKeys', title: 'API Keys', type: 'intro', nav: true },
                { route: 'settings/windowsAuth', moduleId: 'viewModels/windowsAuth', title: 'Windows Authentication', type: 'intro', nav: this.isOnSystemDatabase },
                { route: 'settings/databaseSettings', moduleId: 'viewModels/databaseSettings', title: 'Database Settings', type: 'intro', nav: this.isOnUserDatabase },
                { route: 'settings/periodicBackup', moduleId: 'viewModels/periodicBackup', title: 'Periodic Backup', type: 'intro', nav: this.isOnUserDatabase }
            ])
            .buildNavigationModel();
    }

    activate(args) {
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
    }
}

export = settings;    