import dialog = require("plugins/dialog");
import toggleDatabaseDisabledCommand = require("commands/toggleDatabaseDisabledCommand");
//import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");
import resource = require("models/resource");

class disableDatabaseToggleConfirm extends dialogViewModelBase {

    private resourcesToDisable = ko.observableArray<resource>();
    private disableToggleStarted = false;
    public disableToggleTask = $.Deferred(); // Gives consumers a way to know when the async operation completes.
    private disableOneDatabasePath = "/admin/databases/";
    private disableMultipleDatabasesPath = "/admin/databases/database-batch-toggle-disable";

    desiredAction = ko.observable<string>();
    isSettingDisabled: boolean;
    deletionText: KnockoutComputed<string>;
    confirmDeletionText: KnockoutComputed<string>;

    constructor(resources: Array<resource>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (resources.length === 0) {
            throw new Error("Must have at least one database to disable.");
        }

        this.resourcesToDisable(resources);
        this.isSettingDisabled = !resources[0].disabled();
        this.deletionText = ko.computed(() => this.isSettingDisabled ? "You're disabling" : "You're enabling");
        this.confirmDeletionText = ko.computed(() => this.isSettingDisabled ? "Yep, disable" : "Yep, enable");
    }

    toggleDisableDatabases() {
        var resourceNames = this.resourcesToDisable().map((db: database) => db.name);
        var toggleDisableDatabaseCommand = new toggleDatabaseDisabledCommand(resourceNames, this.isSettingDisabled, this.disableOneDatabasePath, this.disableMultipleDatabasesPath);

        var toggleDisableDatabaseCommandTask = toggleDisableDatabaseCommand.execute();

        toggleDisableDatabaseCommandTask.done((results) => {
            if (this.resourcesToDisable().length == 1) {
                results = [ this.resourcesToDisable()[0].name ];
            }
            this.disableToggleTask.resolve(results);
        });
        toggleDisableDatabaseCommandTask.fail(response => this.disableToggleTask.reject(response));

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

export = disableDatabaseToggleConfirm;