import resource = require("models/resources/resource");

import alertArgs = require("common/alertArgs");
import notificationCenterOperations = require("common/notifications/notificationCenterOperations");
import notificationCenterRecentErrors = require("common/notifications/notificationCenterRecentErrors");

class notificationCenter {
    static instance = new notificationCenter();

    operations = new notificationCenterOperations();
    recentErrors = new notificationCenterRecentErrors();

    monitorOperation<TProgress extends Raven.Client.Data.IOperationProgress,
        TResult extends Raven.Client.Data.IOperationResult>(rs: resource,
        operationId: number,
        onProgress: (progress: TProgress) => void = null): JQueryPromise<TResult> {
        return this.operations.monitorOperation(rs, operationId, onProgress);
    }

    killOperation(operationId: number) {
       this.operations.killOperation(operationId);
    }

    dismissOperation(operationId: number, saveOperations: boolean = true) {
        this.operations.dismissOperation(operationId, saveOperations);
    }

    dismissRecentError(alert: alertArgs) {
        this.recentErrors.dismissRecentError(alert);
    }

}

export = notificationCenter;