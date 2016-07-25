import dialog = require("plugins/dialog");
import disableResourceToggleCommand = require("commands/resources/disableResourceToggleCommand");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import shell = require("viewmodels/shell");
import resource = require("models/resources/resource");

class disableResourceToggleConfirm extends dialogViewModelBase {

    private resourcesToDisable = ko.observableArray<resource>();
    private disableToggleStarted = false;
    public disableToggleTask = $.Deferred(); // Gives consumers a way to know when the async operation completes.

    desiredAction = ko.observable<string>();
    isSettingDisabled: boolean;
    deletionText: KnockoutComputed<string>;
    confirmDeletionText: KnockoutComputed<string>;

    constructor(private resources: Array<resource>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (resources.length === 0) {
            throw new Error("Must have at least one resource to disable.");
        }

        this.resourcesToDisable(resources);
        this.isSettingDisabled = !resources[0].disabled();
        this.deletionText = ko.computed(() => this.isSettingDisabled ? "You're disabling" : "You're enabling");
        this.confirmDeletionText = ko.computed(() => this.isSettingDisabled ? "Yep, disable" : "Yep, enable");
    }

    toggleDisableReources() {
        var selected = this.resources.filter((rs: resource) => rs.isSelected())[0];
        if (!!selected)
            shell.disconnectFromResourceChangesApi();

        var disableToggleCommand = new disableResourceToggleCommand(this.resourcesToDisable(), this.isSettingDisabled);

        var disableToggleCommandTask = disableToggleCommand.execute();

        disableToggleCommandTask.done((results) => {
            if (this.resourcesToDisable().length === 1) {
                results = [ this.resourcesToDisable()[0].name ];
            }
            this.disableToggleTask.resolve(results);
        });
        disableToggleCommandTask.fail(response => {
            if (!!selected)
                selected.activate();

            this.disableToggleTask.reject(response);
        });

        this.disableToggleStarted = true;
        dialog.close(this);
    }

    cancel() {
        dialog.close(this);
    }

    deactivate(args: any) {
        // If we were closed via X button or other dialog dismissal, reject the deletion task since
        // we never carried it out.
        if (!this.disableToggleStarted) {
            this.disableToggleTask.reject();
        }
    }
}

export = disableResourceToggleConfirm;
