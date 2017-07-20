import EVENTS = require("common/constants/events");
import toastr = require("toastr");
import recentError = require("common/notifications/models/recentError");

class messagePublisher {

    private static initialized = false;

    static initialize() {
        if (!messagePublisher.initialized) {
            messagePublisher.initialized = true;

            toastr.options.progressBar = true;
            toastr.options.positionClass = "toast-bottom-right";
            toastr.options.extendedTimeOut = 6000;
            toastr.options.timeOut = 5000;
            toastr.options.showDuration = 100;
            toastr.options.hideDuration = 100;
            toastr.options.showEasing = "linear";
            toastr.options.hideEasing = "linear";
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

    private static reportProgress(type: Raven.Server.NotificationCenter.Notifications.NotificationSeverity, title: string, details?: string, httpStatusText?: string, displayInRecentErrors: boolean = false) {
        const toastrMethod = messagePublisher.getDisplayMethod(type);

        const messageAndOptionalException = recentError.tryExtractMessageAndException(details);
        
        toastrMethod(recentError.trimMessage(messageAndOptionalException.message), title, {
            showDuration: messagePublisher.getDisplayDuration(type),
            closeButton: true
        });

        if (displayInRecentErrors) {
            const error = recentError.create(type, title, details, httpStatusText);

            ko.postbox.publish(EVENTS.NotificationCenter.RecentError, error);
        }
    }

    private static getDisplayDuration(type: Raven.Server.NotificationCenter.Notifications.NotificationSeverity): number {
        return (type === "Error" || type === "Warning") ? 5000 : 2000;
    }

    private static getDisplayMethod(type: Raven.Server.NotificationCenter.Notifications.NotificationSeverity): ToastrDisplayMethod {
        switch (type) {
            case "Success":
                return toastr.success;
            case "Warning":
                return toastr.warning;
            case "Error":
                return toastr.error;
            case "Info":
            case "None":
                return toastr.info;
            default:
                throw new Error("Unhandled alert type = " + type);
        }
    }
}

messagePublisher.initialize();

export = messagePublisher;
