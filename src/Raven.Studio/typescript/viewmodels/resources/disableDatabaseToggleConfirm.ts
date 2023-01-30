import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");
import { DatabaseSharedInfo } from "components/models/databases";

class disableDatabaseToggleConfirm extends confirmViewModelBase<confirmDialogResult> {

    view = require("views/resources/disableDatabaseToggleConfirm.html");

    desiredAction = ko.observable<string>();
    deletionText: string;
    confirmDeletionText: string;

    private readonly databases: DatabaseSharedInfo[];

    private readonly disable: boolean;

    constructor(databases: DatabaseSharedInfo[], disable: boolean) {
        super(null);
        this.disable = disable;
        this.databases = databases;

        this.deletionText = disable ? "You're disabling" : "You're enabling";
        this.confirmDeletionText = disable ? "Disable" : "Enable";
    }
}

export = disableDatabaseToggleConfirm;
