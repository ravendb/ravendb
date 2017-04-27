import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

class indexCompactDetails extends abstractOperationDetails {

    progress: KnockoutObservable<Raven.Server.Documents.Indexes.IndexCompactionProgress>;
    result: KnockoutObservable<Raven.Server.Documents.Indexes.IndexCompactionResult>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);

        this.initObservables();
    }

    initObservables() {
        super.initObservables();

        this.progress = ko.pureComputed(() => {
            return this.op.progress() as Raven.Server.Documents.Indexes.IndexCompactionProgress;
        });

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Server.Documents.Indexes.IndexCompactionResult : null;
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "IndexCompact";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new indexCompactDetails(op, center));
    }

}

export = indexCompactDetails;
