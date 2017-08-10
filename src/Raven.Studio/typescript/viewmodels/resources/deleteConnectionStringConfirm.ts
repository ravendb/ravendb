import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");

class deleteConnectionStringConfirm extends confirmViewModelBase<confirmDialogResult> {

    constructor(private connectionStringType: Raven.Client.ServerWide.ConnectionStringType, private connectionStringName: string) {
        super();
    }
}

export = deleteConnectionStringConfirm;
