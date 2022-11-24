import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

class optimizeIndexDetails extends abstractOperationDetails {

    view = require("views/common/notificationCenter/detailViewer/operations/optimizeIndexDetails.html");

    progress: KnockoutObservable<Raven.Client.ServerWide.Operations.IndexOptimizeResult>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);
        
        this.initObservables();
    }

    initObservables() {
        super.initObservables();
        
        this.progress = ko.pureComputed(() => {
            return (this.op.progress() || this.op.result()) as Raven.Client.ServerWide.Operations.IndexOptimizeResult
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "LuceneOptimizeIndex";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new optimizeIndexDetails(op, center));
    }
}

export = optimizeIndexDetails;
