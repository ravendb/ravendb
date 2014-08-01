import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");

class messagePublisher {
    static reportInfo(title: string, details?: string) {
        this.reportProgress(alertType.info, title, details);
    }

    static reportError(title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = true) {
        this.reportProgress(alertType.danger, title, details, httpStatusText, displayInRecentErrors);
        if (console && console.log && typeof console.log === "function") {
            console.log("Error during command execution", title, details, httpStatusText);
        }
    }

    static reportSuccess(title: string, details?: string) {
        this.reportProgress(alertType.success, title, details);
    }

    static reportWarning(title: string, details?: string, httpStatusText?: string) {
        this.reportProgress(alertType.warning, title, details, httpStatusText);
    }

    private static reportProgress(type: alertType, title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = true) {
        ko.postbox.publish("Alert", new alertArgs(type, title, details, httpStatusText, displayInRecentErrors));
    }
}

export = messagePublisher;