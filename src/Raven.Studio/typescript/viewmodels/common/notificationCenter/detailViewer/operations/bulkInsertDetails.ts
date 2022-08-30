import app = require("durandal/app");

import database = require("models/resources/database");
import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");
import virtualBulkInsert = require("common/notifications/models/virtualBulkInsert");
import virtualBulkInsertFailures = require("common/notifications/models/virtualBulkInsertFailures");

class bulkInsertDetails extends abstractOperationDetails {

    progress: KnockoutObservable<Raven.Client.Documents.Operations.BulkInsertProgress>;
    result: KnockoutObservable<Raven.Client.Documents.Operations.BulkOperationResult>;
    processingSpeed: KnockoutComputed<string>;

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);

        this.initObservables();
    }

    initObservables() {
        super.initObservables();

        this.progress = ko.pureComputed(() => {
            return this.op.progress() as Raven.Client.Documents.Operations.BulkInsertProgress;
        });

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Client.Documents.Operations.BulkOperationResult : null;
        });

        this.processingSpeed = ko.pureComputed(() => {
            const progress = this.progress();
            if (!progress) {
                return "N/A";
            }
            
            const processingSpeed = this.calculateProcessingSpeed(progress.DocumentsProcessed);
            if (processingSpeed === 0) {
                return "N/A";
            }

            return `${processingSpeed.toLocaleString()} docs / sec`;
        }).extend({ rateLimit : 2000 });
    }
    
    static tryHandle(operationDto: Raven.Server.NotificationCenter.Notifications.OperationChanged, notificationsContainer: KnockoutObservableArray<abstractNotification>,
                     database: database, callbacks: { spinnersCleanup: Function, onChange: Function }): boolean {
        
        if (operationDto.Type === "OperationChanged" && operationDto.TaskType === "BulkInsert") {
            if (operationDto.State.Status === "Completed") {
                abstractOperationDetails.handleInternal(virtualBulkInsert, operationDto, notificationsContainer, database, callbacks);
                return true;
            }

            if (operationDto.State.Status === "Faulted") {
                abstractOperationDetails.handleInternal(virtualBulkInsertFailures, operationDto, notificationsContainer, database, callbacks);
                return true;
            }
        }
        
        return false;
    }

    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && notification.taskType() === "BulkInsert";
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new bulkInsertDetails(op, center));
    }

}

export = bulkInsertDetails;
