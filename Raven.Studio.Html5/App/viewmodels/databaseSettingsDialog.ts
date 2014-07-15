import app = require("durandal/app");
import dialog = require("plugins/dialog");
import appUrl = require("common/appUrl");
import viewModelBase = require("viewmodels/viewModelBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");

class databaseSettingsDialog extends dialogViewModelBase {

    public dialogTask = $.Deferred();

    routes: Array<{title:string; moduleId:string}>;
    appUrls: computedAppUrls;
    activeScreen: KnockoutObservable<string> = ko.observable<string>("");
    activeModel: KnockoutObservable<viewModelBase> = ko.observable<viewModelBase>(null);

    bundleMap = { quotas: "Quotas", versioning: "Versioning" };
    userDatabasePages = ko.observableArray([]);

    dirtyFlag = new ko.DirtyFlag([]);

    constructor(bundles: Array<string>) {
        super();

        this.appUrls = appUrl.forCurrentDatabase();

        var quotasRoute = { moduleId: 'viewmodels/quotas', title: 'Quotas' };
        var versioningRoute = { moduleId: 'viewmodels/versioning', title: 'Versioning' };

        // when the activeScreen name changes - load the viewmodel
        this.activeScreen.subscribe((newValue) => 
            require([newValue], (model) => this.activeModel(new model()))
            );

        this.routes = [];
        if (bundles.contains("Quotas")) {
            this.routes.push(quotasRoute);
        }
        if (bundles.contains("Versioning")) {
            this.routes.push(versioningRoute);
        }
    }

    attached() {
        this.dirtyFlag().reset();
        this.showView(this.routes[0].moduleId);
    }

    detached() {
        this.dialogTask.resolve();
    }

    checkDirtyFlag(yesCallback: Function, noCallback?: Function) {
        var deferred: JQueryPromise<string>;
        if (this.dirtyFlag().isDirty()) {
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