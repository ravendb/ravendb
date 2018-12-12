import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

class generateClientCertificateDetails extends abstractOperationDetails {

    result: KnockoutObservable<Raven.Client.ServerWide.Operations.Certificates.ClientCertificateGenerationResult>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);

        this.initObservables();
    }

    initObservables() {
        super.initObservables();

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Client.ServerWide.Operations.Certificates.ClientCertificateGenerationResult : null;
        });
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "CertificateGeneration";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new generateClientCertificateDetails(op, center));
    }

}

export = generateClientCertificateDetails;
