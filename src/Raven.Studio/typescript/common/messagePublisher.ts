import alertType = require("common/alertType");
import alertArgs = require("common/alertArgs");
import EVENTS = require("common/constants/events");

class messagePublisher {
    static reportInfo(title: string, details?: string) {
        this.reportProgress(alertType.info, title, details);
    }

    static reportError(title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = true) {
        this.reportProgress(alertType.danger, title, details, httpStatusText, displayInRecentErrors);
        console.log("Error during command execution", title, details, httpStatusText);
    }

    static reportSuccess(title: string, details?: string) {
        this.reportProgress(alertType.success, title, details);
    }

    static reportWarning(title: string, details?: string, httpStatusText?: string) {
        this.reportProgress(alertType.warning, title, details, httpStatusText);
    }

    static reportWarningWithButton(title: string, details: string, buttonName: string, action: () => any) {
        var alert = new alertArgs(alertType.warning, title, details, null, true);
        alert.setupButtton(buttonName, action);
        ko.postbox.publish(EVENTS.NotificationCenter.Alert, alert);
    }

    private static reportProgress(type: alertType, title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = true) {
        ko.postbox.publish(EVENTS.NotificationCenter.Alert, new alertArgs(type, title, details, httpStatusText, displayInRecentErrors));
    }
}

export = messagePublisher;
