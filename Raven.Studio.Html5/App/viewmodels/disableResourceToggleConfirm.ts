import dialog = require("plugins/dialog");
import disableResourceToggleCommand = require("commands/disableResourceToggleCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import resource = require("models/resource");
import database = require("models/database");
import filesystem = require("models/filesystem/filesystem");

class disableResourceToggleConfirm extends dialogViewModelBase {

    private resourcesToDisable = ko.observableArray<resource>();
    private disableToggleStarted = false;
    public disableToggleTask = $.Deferred(); // Gives consumers a way to know when the async operation completes.

    desiredAction = ko.observable<string>();
    isSettingDisabled: boolean;
    deletionText: KnockoutComputed<string>;
    confirmDeletionText: KnockoutComputed<string>;
    resourceType: string;
    resourcesTypeText: string;

    constructor(resources: Array<resource>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (resources.length === 0) {
            throw new Error("Must have at least one resource to disable.");
        }

        this.resourcesToDisable(resources);
        this.resourceType = resources[0].type;
        this.resourcesTypeText = this.resourceType == database.type ? 'databases' : this.resourceType == filesystem.type ? 'file systems' : 'counter storages';
        this.isSettingDisabled = !resources[0].disabled();
        this.deletionText = ko.computed(() => this.isSettingDisabled ? "You're disabling" : "You're enabling");
        this.confirmDeletionText = ko.computed(() => this.isSettingDisabled ? "Yep, disable" : "Yep, enable");
    }

    toggleDisableReources() {
        var resourceNames = this.resourcesToDisable().map((rs: resource) => rs.name);
        var disableToggleCommand = new disableResourceToggleCommand(resourceNames, this.isSettingDisabled, this.resourceType);

        var disableToggleCommandTask = disableToggleCommand.execute();

        disableToggleCommandTask.done((results) => {
            if (this.resourcesToDisable().length == 1) {
                results = [ this.resourcesToDisable()[0].name ];
            }
            this.disableToggleTask.resolve(results);
        });
        disableToggleCommandTask.fail(response => this.disableToggleTask.reject(response));

        this.disableToggleStarted = true;
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate(args) {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never carried it out.
        if (!this.disableToggleStarted) {
            this.disableToggleTask.reject();
        }
    }
}

export = disableResourceToggleConfirm;