import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");

class deleteTransformationScriptConfirm extends confirmViewModelBase<confirmDialogResult> {

    constructor(private db: database, private transformationScriptName: string) {
        super();
    }
}

export = deleteTransformationScriptConfirm;
