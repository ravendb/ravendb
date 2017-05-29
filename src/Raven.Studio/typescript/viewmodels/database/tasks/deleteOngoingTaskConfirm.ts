import confirmViewModelBase = require("viewmodels/confirmViewModelBase");
import database = require("models/resources/database");
import dialog = require("plugins/dialog");
import deleteOngoingTaskCommand = require("commands/database/tasks/deleteOngoingTaskCommand");
import messagePublisher = require("common/messagePublisher");

class deleteOngoingTaskConfirm extends confirmViewModelBase<deleteDatabaseConfirmResult> {
   
    deleteTask = $.Deferred<boolean>();
    taskTypeStr = ko.observable<Raven.Server.Web.System.OngoingTaskType>("SQL");

    constructor(private db: database, taskType: Raven.Server.Web.System.OngoingTaskType, private taskId: number) {
        super();
        this.taskTypeStr(taskType);
    }

    deleteTaskOperation() {

        const deleteCommand = new deleteOngoingTaskCommand(this.db, this.taskTypeStr(), this.taskId);
        deleteCommand.execute()
            .done(() => {
                messagePublisher.reportSuccess("Successfully deleted " + this.taskTypeStr() + " task");
            })
            .fail(() => {
                messagePublisher.reportError("Failed to delete " + this.taskTypeStr() + " task");
                this.deleteTask.reject();
            });

        dialog.close(this);
    }

    cancel() { 
        this.deleteTask.resolve(false); // ???
        dialog.close(this);
    }
}

export = deleteOngoingTaskConfirm;
