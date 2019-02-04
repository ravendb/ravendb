
import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");
import appUrl = require("common/appUrl");
import router = require("plugins/router");

class transactionCommandsDetails extends abstractOperationDetails {

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);

        this.initObservables();
    }

    static tryHandle(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged): boolean {
        if (operationDto.TaskType !== "RecordTransactionCommands") {
            return false;
        }
        
        // if operation is in progress then we add empty progress, so notification center, can display 'details' 
        // button. Since this operation only goes to view 'show details' action we don't need anything meaningful in progress object.
        
        // we return false, so operation is not marked as processed - it is handed by standard flow. 
        
        if (operationDto.State.Status === "InProgress") {
            operationDto.State.Progress = {};
        }
        
        return false;
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "RecordTransactionCommands";
    }

    static showDetailsFor(op: operation) {
        const description = op.detailedDescription() as Raven.Server.Documents.Handlers.TransactionsRecordingHandler.RecordingDetails;
        router.navigate(appUrl.forDebugAdvancedRecordTransactionCommands(description.DatabaseName));
    }

}

export = transactionCommandsDetails;
