import configurationKey = require("models/filesystem/configurationKey");
import filesystem = require("models/filesystem/filesystem");
import dialog = require("plugins/dialog");
import deleteConfigurationKeyCommand = require("commands/filesystem/deleteConfigurationKeyCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");

class deleteConfigurationKeys extends dialogViewModelBase {

    private keys = ko.observableArray<configurationKey>();
    private deletionStarted = false;
    public deletionTask = $.Deferred(); // Gives consumers a way to know when the async delete operation completes.

    constructor(public fs : filesystem, keys: Array<configurationKey>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (keys.length === 0) {
            throw new Error("Must have at least one key to delete.");
        }

        this.keys(keys);
    }

    deleteKeys() {
        this.deletionStarted = true;
        var deleteItemsIds = this.keys().map(i => i.key);
        var deletionTasks: Array<JQueryPromise<any>> = [];
        for (var i = 0; i < deleteItemsIds.length; i++) {
            deletionTasks.push(new deleteConfigurationKeyCommand(this.fs, deleteItemsIds[i]).execute());
        }

        var combinedTask = $.when.apply($, deletionTasks);

        combinedTask
            .done(() => this.deletionTask.resolve(this.keys()))
            .fail((response: any) => this.deletionTask.reject(response));

        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate(args: any) {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never carried it out.
        if (!this.deletionStarted) {
            this.deletionTask.reject();
        }
    }
}

export = deleteConfigurationKeys;
