import app = require("durandal/app");

import operation = require("common/notifications/models/operation");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import abstractOperationDetails = require("viewmodels/common/notificationCenter/detailViewer/operations/abstractOperationDetails");

class reshardingDetails extends abstractOperationDetails {

    //TODO: handle error state
    
    view = require("views/common/notificationCenter/detailViewer/operations/reshardingDetails.html");

    progress: KnockoutObservable<Raven.Server.Documents.Sharding.Handlers.ReshardingHandler.ReshardingResult>;
    result: KnockoutObservable<Raven.Server.Documents.Sharding.Handlers.ReshardingHandler.ReshardingResult>;
    
    message = ko.observable<string>();

    constructor(op: operation, notificationCenter: notificationCenter) {
        super(op, notificationCenter);

        this.initObservables();
    }

    initObservables() {
        super.initObservables();
        
        this.progress = ko.pureComputed(() => {
            return this.op.progress() as Raven.Server.Documents.Sharding.Handlers.ReshardingHandler.ReshardingResult;
        });

        this.result = ko.pureComputed(() => {
            return this.op.status() === "Completed" ? this.op.result() as Raven.Server.Documents.Sharding.Handlers.ReshardingHandler.ReshardingResult : null;
        });
        
        this.message = ko.pureComputed(() => {
            const completed = this.op.status() === "Completed";
            
            if (completed) {
                const result = this.op.result() as Raven.Server.Documents.Sharding.Handlers.ReshardingHandler.ReshardingResult;
                return result.Message;
            }
            
            const progress = this.op.progress() as Raven.Server.Documents.Sharding.Handlers.ReshardingHandler.ReshardingResult;
            if (progress) {
                return progress.Message;
            }
            
            return "";
        });
    }
    
    static supportsDetailsFor(notification: abstractNotification) {
        return (notification instanceof operation) && (notification.taskType() === "Resharding");
    }

    static showDetailsFor(op: operation, center: notificationCenter) {
        return app.showBootstrapDialog(new reshardingDetails(op, center));
    }

}

export = reshardingDetails;
