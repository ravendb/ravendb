import app = require("durandal/app");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import durandalRouter = require("plugins/router");

class databaseSettingsDialog extends dialogViewModelBase {

    public dialogTask = $.Deferred();
    router: DurandalRootRouter = null;
    routes: Array<{title:string; moduleId:string}>;
    appUrls: computedAppUrls;
    activeScreen: KnockoutObservable<string> = ko.observable<string>("");
    activeModel: KnockoutObservable<viewModelBase> = ko.observable<viewModelBase>(null);

    bundleMap = { quotas: "Quotas", versioning: "Versioning" };
    userDatabasePages = ko.observableArray([]);

    constructor(bundles: Array<string>) {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        var quotasRoute = { moduleId: 'viewmodels/quotas', title: 'Quotas', activate: true};
        var versioningRoute = { moduleId: 'viewmodels/versioning', title: 'Versioning', activate: true};
        var sqlReplicationConnectionRoute = { moduleId: 'viewmodels/sqlReplicationConnectionStringsManagement', title: 'SQL Replication Connection Strings', activate: true};

        // when the activeScreen name changes - load the viewmodel
        this.activeScreen.subscribe((newValue) => 
            require([newValue], (model) => {
                this.activeModel(new model());

            })
            );

        this.activeModel.subscribe(() => {
            debugger;
        });

        this.routes = [];
        if (bundles.contains("Quotas")) {
            this.routes.push(quotasRoute);
        }
        if (bundles.contains("Versioning")) {
            this.routes.push(versioningRoute);
        }
        if (bundles.contains("SqlReplication")) {
            this.routes.push(sqlReplicationConnectionRoute);
        }

//        var apiKeyRoute = { route: 'databases/settings/apiKeys', moduleId: 'viewmodels/quotas', title: 'API Keys', nav: true, hash: appUrl.forApiKeys() };
//        var windowsAuthRoute = { route: 'databases/settings/windowsAuth', moduleId: 'viewmodels/versioning', title: 'Windows Authentication', nav: true, hash: appUrl.forWindowsAuth() };
//        var databaseSettingsRoute = { route: ['databases/settings', 'databases/settings/databaseSettings'], moduleId: 'viewmodels/databaseSettings', title: 'Database Settings', nav: true, hash: appUrl.forCurrentDatabase().databaseSettings };
//        var quotasRoute = { route: 'databases/settings/quotas', moduleId: 'viewmodels/quotas', title: 'Quotas', nav: true, hash: appUrl.forCurrentDatabase().quotas };
//        var replicationsRoute = { route: 'databases/settings/replication', moduleId: 'viewmodels/replications', title: 'Replication', nav: true, hash: appUrl.forCurrentDatabase().replications };
//        var sqlReplicationsRoute = { route: 'databases/settings/sqlReplication', moduleId: 'viewmodels/sqlReplications', title: 'SQL Replication', nav: true, hash: appUrl.forCurrentDatabase().sqlReplications };
//        var editsqlReplicationsRoute = { route: 'databases/settings/editSqlReplication(/:sqlReplicationName)', moduleId: 'viewmodels/editSqlReplication', title: 'Edit SQL Replication', nav: true, hash: appUrl.forCurrentDatabase().editSqlReplication };
//        var sqlReplicationsConnectionsRoute = { route: 'databases/settings/sqlReplicationConnectionStringsManagement', moduleId: 'viewmodels/sqlReplicationConnectionStringsManagement', title: 'SQL Replication Connection Strings', nav: true, hash: appUrl.forCurrentDatabase().sqlReplicationsConnections };
//        var versioningRoute = { route: 'databases/settings/versioning', moduleId: 'viewmodels/versioning', title: 'Versioning', nav: true, hash: appUrl.forCurrentDatabase().versioning };
//        var periodicExportRoute = { route: 'databases/settings/periodicExports', moduleId: 'viewmodels/periodicExport', title: 'Periodic Export', nav: true, hash: appUrl.forCurrentDatabase().periodicExport };
//        //var scriptedIndexesRoute = { route: 'databases/settings/scriptedIndex', moduleId: 'viewmodels/scriptedIndexes', title: 'Scripted Index', nav: true, hash: appUrl.forCurrentDatabase().scriptedIndexes };
//        var customFunctionsEditorRoute = { route: 'databases/settings/customFunctionsEditor', moduleId: 'viewmodels/customFunctionsEditor', title: 'Custom Functions', nav: true, hash: appUrl.forCurrentDatabase().customFunctionsEditor };
//
//
//        this.router = durandalRouter.createChildRouter()
//            .map([
//                apiKeyRoute,
//                windowsAuthRoute,
//                databaseSettingsRoute,
//                quotasRoute,
//                replicationsRoute,
//                sqlReplicationsRoute,
//                sqlReplicationsConnectionsRoute,
//                editsqlReplicationsRoute,
//                versioningRoute,
//                periodicExportRoute,
//            //scriptedIndexesRoute,
//                customFunctionsEditorRoute
//            ])
//            .buildNavigationModel();
    }

    attached() {
        viewModelBase.dirtyFlag().reset();
        this.showView(this.routes[0].moduleId);
    }

    detached() {
        this.dialogTask.resolve();
    }

    checkDirtyFlag(yesCallback: Function, noCallback?: Function) {
        var deferred: JQueryPromise<string>;
        if (viewModelBase.dirtyFlag().isDirty()) {
            deferred = app.showMessage('You have unsaved data. Are you sure you want to close?', 'Unsaved Data', ['Yes', 'No']);
        } else {
            deferred = $.Deferred().resolve("Yes");
        }

        deferred.done((canDo: string) => {
            if (canDo === "Yes" && yesCallback) {
                yesCallback();
            } else if (canDo === "No" && noCallback) {
                noCallback();
            }
        });
    }

    showView(moduleId: string) {
        this.checkDirtyFlag(() => this.activeScreen(moduleId));
    }

    isActive(moduleId: string) {
        return moduleId === this.activeScreen();
    }

    close() {
        this.checkDirtyFlag(() => dialog.close(this));
    }
}

export = databaseSettingsDialog;