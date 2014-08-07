import app = require("durandal/app");
import dialog = require("plugins/dialog");
import viewModelBase = require("viewmodels/viewModelBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import activator = require("durandal/activator");

class databaseSettingsDialog extends dialogViewModelBase {

    public dialogTask = $.Deferred();
    content: DurandalActivator<any>;
    currentModel: viewModelBase;
    routes: Array<{ title: string; moduleId: string }>;

    constructor(moduleId: string, private title: string) {
        super();

        this.routes = [
            { moduleId: 'viewmodels/apiKeys', title: 'API Keys', activate: true },
            { moduleId: 'viewmodels/windowsAuth', title: 'Windows Authentication', activate: true },
            { moduleId: 'viewmodels/restoreDatabase', title: 'Restore Database', activate: true }
        ];

        this.content = activator.create();

        require([moduleId], (model) => {
            this.currentModel = new model();
            this.content.activateItem(this.currentModel);
        });
    }

    attache() {
        super.attached();
    }

    detached() {
        super.detached();
        this.dialogTask.resolve();
    }

    canDeactivate(): any {
        var isDirty = this.currentModel.dirtyFlag().isDirty();
        
        if (isDirty) {
            return app.showMessage('You have unsaved data. Are you sure you want to close?', 'Unsaved Data', ['No', 'Yes']);
        }

        return true;
    }

    close() {
        dialog.close(this);
    }
}

export = databaseSettingsDialog;