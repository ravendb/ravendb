import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");

class disableDatabaseToggleConfirm extends confirmViewModelBase<confirmDialogResult> {

    view = require("views/resources/disableDatabaseToggleConfirm.html");

    desiredAction = ko.observable<string>();
    deletionText: string;
    confirmDeletionText: string;

    constructor(private databases: Array<database>, private disable: boolean) {
        super(null);

        this.deletionText = disable ? "You're disabling" : "You're enabling";
        this.confirmDeletionText = disable ? "Disable" : "Enable";
    }
}

export = disableDatabaseToggleConfirm;
