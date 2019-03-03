import EVENTS = require("common/constants/events");
import toastr = require("toastr");
import recentError = require("common/notifications/models/recentError");
import recentLicenseLimitError = require("common/notifications/models/recentLicenseLimitError");
import generalUtils = require("common/generalUtils");

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

        let error: recentError;
        
        if (displayInRecentErrors) {
            error = messagePublisher.createRecentError(type, title, details, httpStatusText);

            ko.postbox.publish(EVENTS.NotificationCenter.RecentError, error);
        }
        
        let message = generalUtils.escapeHtml(
            generalUtils.trimMessage(messageAndOptionalException.message)
        );
        
        if (error && error.hasDetails()) {
            const extraHtml = "<br /><small>show details</small>";
            message = message ? message + extraHtml : extraHtml;
        }
        
        const escapedTitle = generalUtils.escapeHtml(title);
        
        toastrMethod(message, escapedTitle, {
            showDuration: messagePublisher.getDisplayDuration(type),
            closeButton: true,
            
            onclick: (error && error.hasDetails()) ? () => {
                ko.postbox.publish(EVENTS.NotificationCenter.OpenNotification, error);
            }: undefined 
        });
    }

    static createRecentError(severity: Raven.Server.NotificationCenter.Notifications.NotificationSeverity, title: string, details: string, httpStatus: string) {
        const messageAndException = recentError.tryExtractMessageAndException(details);

        const dto = {
            CreatedAt: null,
            IsPersistent: false,
            Title: title,
            Message: messageAndException.message,
            Id: "RecentError/" + (recentError.currentErrorId++),
            Type: "RecentError",
            Details: messageAndException.error,
            HttpStatus: httpStatus,
            Severity: severity,
        } as recentErrorDto;

        if (httpStatus === "Payment Required") {
            const licenseType = recentLicenseLimitError.tryExtractLicenseLimitType(details);
            return new recentLicenseLimitError(dto, licenseType);
        } else {
            return new recentError(dto);
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
