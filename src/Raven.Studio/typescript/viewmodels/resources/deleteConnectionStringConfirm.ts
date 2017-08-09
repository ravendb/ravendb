import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");

class deleteConnectionStringConfirm extends confirmViewModelBase<confirmDialogResult> {

    type = ko.observable<Raven.Client.ServerWide.ConnectionStringType>();

    constructor(private connectionStringType: Raven.Client.ServerWide.ConnectionStringType, private connectionStringName: string) {
        super();

        this.type(connectionStringType);
    }
}

export = deleteConnectionStringConfirm;
