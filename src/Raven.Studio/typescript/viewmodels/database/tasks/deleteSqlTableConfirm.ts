import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");

class deleteSqlTableConfirm extends confirmViewModelBase<confirmDialogResult> {

    constructor(private db: database, private sqlTableName: string) {
        super();
    }
}

export = deleteSqlTableConfirm;
