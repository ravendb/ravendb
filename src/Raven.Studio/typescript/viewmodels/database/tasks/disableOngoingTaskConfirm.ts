import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");

class disableOngoingTaskConfirm extends confirmViewModelBase<confirmDialogResult> {
   
    constructor(private db: database, private taskType: Raven.Client.ServerWide.Operations.OngoingTaskType, private taskId: number) {
        super();
    }
}

export = disableOngoingTaskConfirm;
