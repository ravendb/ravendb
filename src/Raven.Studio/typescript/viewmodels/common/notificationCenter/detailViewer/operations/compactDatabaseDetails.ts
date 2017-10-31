import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");



class compactDatabaseDetails extends abstractOperationDetails {

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        this.initObservables();
    }
    
    /* TODO:
    progress: KnockoutObservable<Raven.Server.Documents.DatabaseCompactionProgress>;
    result: KnockoutObservable<Raven.Server.Documents.DatabaseCompactionResult>;


    initObservables() {
        super.initObservables();

        this.progress = ko.pureComputed(() => {
            return this.op.progress() as Raven.Server.Documents.DatabaseCompactionProgress;
        });

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Server.Documents.DatabaseCompactionResult : null;
        });
    }*/

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "DatabaseCompact";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new compactDatabaseDetails(op, center));
    }

    /* TODO
    static merge(existing: operation, incoming: Raven.Server.NotificationCenter.Notifications.OperationChanged): void {
        if (!smugglerDatabaseDetails.supportsDetailsFor(existing)) {
            return;
        }

        const isUpdate = !_.isUndefined(incoming);

        if (!isUpdate) {
            // object was just created  - only copy message -> message field

            if (!existing.isCompleted()) {
                const result = existing.progress() as Raven.Client.Documents.Smuggler.SmugglerResult;
                result.Messages = [result.Message];
            }

        } else if (incoming.State.Status === "InProgress") { // if incoming operaton is in progress, then merge messages into existing item
            const incomingResult = incoming.State.Progress as Raven.Client.Documents.Smuggler.SmugglerResult;
            const existingResult = existing.progress() as Raven.Client.Documents.Smuggler.SmugglerResult;

            incomingResult.Messages = existingResult.Messages.concat(incomingResult.Message);
        }

        if (isUpdate) {
            existing.updateWith(incoming);
        }
    }*/
    
}

export = compactDatabaseDetails;
