import dialog = require("plugins/dialog");
import viewModelBase = require("viewmodels/viewModelBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import activator = require("durandal/activator");

class filesystemSettingsDialog extends dialogViewModelBase {

    public dialogTask = $.Deferred();
    routes: Array<{ title: string; moduleId: string }>;
    activeScreen: KnockoutObservable<string> = ko.observable<string>("");
    content: DurandalActivator<any>;
    currentModel: viewModelBase;

    constructor(bundles: Array<string>) {
        super();

        this.content = activator.create();

        var versioningRoute = { moduleId: 'viewmodels/filesystem/settings/versioning', title: 'Versioning', activate: true };

        // when the activeScreen name changes - load the viewmodel
        this.activeScreen.subscribe((newValue) =>
            require([newValue], (model: new() => viewModelBase) => {
                this.currentModel = new model();
                this.content.activateItem(this.currentModel);
            })
            );

        this.routes = [];
        if (bundles.contains("Versioning")) {
            this.routes.push(versioningRoute);
        }
    }

    attached() {
        super.attached();
        this.activeScreen(this.routes[0].moduleId);
    }

    detached() {
        super.detached();
        this.dialogTask.resolve();
    }

    canDeactivate(): any {
        return this.currentModel.canDeactivate(false);
    }

    showView(moduleId: string) {
        var canDeactivate = this.canDeactivate();

        if (canDeactivate.done) {
            canDeactivate.done((answer: any) => {
                if (answer.can) {
                    this.onSuccessfulDeactivation(moduleId);
                }
            });
        } else if (canDeactivate === true) {
            this.onSuccessfulDeactivation(moduleId);
        }
    }

    onSuccessfulDeactivation(moduleId: string) {
        this.currentModel.dirtyFlag().reset();
        this.activeScreen(moduleId);
    }

    isActive(moduleId: string) {
        return moduleId === this.activeScreen();
    }

    close() {
        dialog.close(this);
    }
}

export = filesystemSettingsDialog;
