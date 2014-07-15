import dialog = require("plugins/dialog");
import toggleDatabaseDisabledCommand = require("commands/toggleDatabaseDisabledCommand");
//import appUrl = require("common/appUrl");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import database = require("models/database");

class disableDatabaseToggleConfirm extends dialogViewModelBase {

    private databasesToDisable = ko.observableArray<database>();
    private disableToggleStarted = false;
    public disableToggleTask = $.Deferred(); // Gives consumers a way to know when the async operation completes.

    desiredAction = ko.observable<string>();
    isSettingDisabled: boolean;
    deletionText: KnockoutComputed<string>;
    confirmDeletionText: KnockoutComputed<string>;

    constructor(databases: Array<database>, elementToFocusOnDismissal?: string) {
        super(elementToFocusOnDismissal);

        if (databases.length === 0) {
            throw new Error("Must have at least one database to disable.");
        }

        this.databasesToDisable(databases);
        this.isSettingDisabled = !databases[0].disabled();
        this.deletionText = ko.computed(() => this.isSettingDisabled ? "You're disabling" : "You're enabling");
        this.confirmDeletionText = ko.computed(() => this.isSettingDisabled ? "Yep, disable" : "Yep, enable");
    }

    toggleDisableDatabases() {
        var databaseNames = this.databasesToDisable().map((db: database) => db.name);
        var toggleDisableDatabaseCommand = new toggleDatabaseDisabledCommand(databaseNames, this.isSettingDisabled);

        var toggleDisableDatabaseCommandTask = toggleDisableDatabaseCommand.execute();

        toggleDisableDatabaseCommandTask.done((results) => {
            if (this.databasesToDisable().length == 1) {
                results = [ this.databasesToDisable()[0].name ];
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