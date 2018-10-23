import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

class replayTransactionCommandsDetails extends abstractOperationDetails {

    result: KnockoutObservable<Raven.Server.Documents.ClientCertificateGenerationResult>;
    progress: KnockoutObservable<Raven.Client.Documents.Operations.IndeterminateProgressCount>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);

        this.initObservables();
    }

    initObservables() {
        super.initObservables();


        this.progress = ko.pureComputed(() => {
            return this.op.progress() as Raven.Client.Documents.Operations.IndeterminateProgressCount;
        });

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Client.Documents.Operations.TransactionsRecording.ReplayTxOperationResult : null;
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "ReplayTransactionCommands";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new replayTransactionCommandsDetails(op, center));
    }

}

export = replayTransactionCommandsDetails;
