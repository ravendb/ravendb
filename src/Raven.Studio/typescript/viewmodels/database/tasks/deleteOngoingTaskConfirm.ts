import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");

class deleteOngoingTaskConfirm extends confirmViewModelBase<confirmDialogResult> {
   
    constructor(private db: database, private taskType: Raven.Server.Web.System.OngoingTaskType, private taskId: number) {
        super();
    }

}

export = deleteOngoingTaskConfirm;
