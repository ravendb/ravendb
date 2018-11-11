import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

class deleteDocumentsDetails extends abstractOperationDetails {

    progress: KnockoutObservable<Raven.Client.Documents.Operations.DeterminateProgress>;
    result: KnockoutObservable<Raven.Client.Documents.Operations.BulkOperationResult>;
    processingSpeed: KnockoutComputed<string>;
    estimatedTimeLeft: KnockoutComputed<string>;

    query: string;
    deleteTypeName: string;
    taskType: Raven.Server.Documents.Operations.Operations.OperationType;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        this.taskType = op.taskType();
        this.deleteTypeName = this.taskType === "DeleteByCollection" ? "Collection" : this.taskType === "DeleteByQuery" ? "Index" :
            "N/A";

        this.initObservables();
    }

    initObservables() {
        super.initObservables();

        if (this.taskType === "DeleteByQuery") {
            this.query = (this.op.detailedDescription() as Raven.Client.Documents.Operations.BulkOperationResult.OperationDetails).Query;
        }

        this.progress = ko.pureComputed(() => {
            return this.op.progress() as Raven.Client.Documents.Operations.DeterminateProgress;
        });

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Client.Documents.Operations.BulkOperationResult : null;
        });

        this.processingSpeed = ko.pureComputed(() => {
            const progress = this.progress();
            if (!progress) {
                return "N/A";
            }
            const processingSpeed = this.calculateProcessingSpeed(progress.Processed);
            if (processingSpeed === 0) {
                return "N/A";
            }

            return `${processingSpeed.toLocaleString()} docs / sec`;
        }).extend({ rateLimit : 2000 });

        this.estimatedTimeLeft = ko.pureComputed(() => {
            const progress = this.progress();
            if (!progress) {
                return "N/A";
            }
            return this.getEstimatedTimeLeftFormatted(progress.Processed, progress.Total);
        }).extend({ rateLimit : 2000 });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && (notification.taskType() === "DeleteByCollection" || notification.taskType() === "DeleteByQuery");
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new deleteDocumentsDetails(op, center));
    }

}

export = deleteDocumentsDetails;
