import raven = require("common/raven");
import alertArgs = require("common/alertArgs");
import alertType = require("common/alertType");

/// Commands encapsulate a write operation to the database and support progress notifications.
class commandBase {
    ravenDb: raven;

    constructor() {
        this.ravenDb = new raven();
    }

    execute(): JQueryPromise<any> {
        throw new Error("Execute must be overridden.");
    }

    reportInfo(title: string, details?: string) {
        this.reportProgress(alertType.info, title, details);
    }

    reportError(title: string, details?: string) {
        this.reportProgress(alertType.danger, title, details);
    }

    reportSuccess(title: string, details?: string) {
        this.reportProgress(alertType.success, title, details);
    }

    reportWarning(title: string, details?: string) {
        this.reportProgress(alertType.warning, title, details);
    }

    private reportProgress(type: alertType, title: string, details?: string) {
        ko.postbox.publish("Alert", new alertArgs(type, title, details));
    }
}

export = commandBase;