import alertArgs = require("common/alertArgs");
import EVENTS = require("common/constants/events");
import toastr = require("toastr");

class messagePublisher {

    private static initialized = false;

    static initialize() {
        if (!messagePublisher.initialized) {
            messagePublisher.initialized = true;

            toastr.options.progressBar = true;
            toastr.options.positionClass = "toast-bottom-right";
        }
    }

    static reportInfo(title: string, details?: string) {
        this.reportProgress("Info", title, details);
    }

    static reportError(title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = true) {
        this.reportProgress("Error", title, details, httpStatusText, displayInRecentErrors);
        console.error("Error during command execution", title, details, httpStatusText);
    }

    static reportSuccess(title: string, details?: string) {
        this.reportProgress("Success", title, details);
    }

    static reportWarning(title: string, details?: string, httpStatusText?: string) {
        this.reportProgress("Warning", title, details, httpStatusText);
    }

    private static reportProgress(type: alertType, title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = false) {
        const alert = new alertArgs(type, title, details, httpStatusText);

        const toastrMethod = messagePublisher.getDisplayMethod(alert.type);

        toastrMethod(alert.errorMessage, alert.title, {
            showDuration: messagePublisher.getDisplayDuration(alert.type),
            closeButton: true
        });

        if (displayInRecentErrors) {
            ko.postbox.publish(EVENTS.NotificationCenter.RecentError, alert);
        }
    }

    private static getDisplayDuration(type: alertType): number {
        return (type === "Error" || type === "Warning") ? 5000 : 2000;
    }

    private static getDisplayMethod(type: alertType): ToastrDisplayMethod {
        switch (type) {
            case "Success":
                return toastr.success;
            case "Warning":
                return toastr.warning;
            case "Error":
                return toastr.error;
            case "Info":
                return toastr.info;
            default:
                throw new Error("Unhandled alert type = " + type);
        }
    }
}

messagePublisher.initialize();

export = messagePublisher;
