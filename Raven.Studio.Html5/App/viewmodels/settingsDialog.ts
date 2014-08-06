import app = require("durandal/app");
import dialog = require("plugins/dialog");
import viewModelBase = require("viewmodels/viewModelBase");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import activator = require("durandal/activator");

class databaseSettingsDialog extends dialogViewModelBase {

    public dialogTask = $.Deferred();
    content: DurandalActivator<any>;
    currentModel: viewModelBase;

    constructor(moduleId: string, private title: string) {
        super();

        this.content = activator.create();

        require([moduleId], (model) => {
            this.currentModel = new model();
            this.content.activateItem(this.currentModel);
        });
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