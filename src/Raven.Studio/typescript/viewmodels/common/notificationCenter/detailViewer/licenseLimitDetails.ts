import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import recentLicenseLimitError = require("common/notifications/models/recentLicenseLimitError");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import alert = require("common/notifications/models/alert");
import license = require("models/auth/licenseModel");
import registration = require("viewmodels/shell/registration");

class licenseLimitDetails extends dialogViewModelBase {

    view = require("views/common/notificationCenter/detailViewer/licenseLimitDetails.html");

    protected readonly licenseLimitNotification: recentLicenseLimitError | alert;
    protected readonly dismissFunction: () => void;
    licenseStatus = license.licenseStatus;
    requestLicenseUrl: KnockoutComputed<string>;
    licenseRequestType: KnockoutComputed<string>;

    constructor(licenseLimitNotification: alert | recentLicenseLimitError, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close");

        this.licenseLimitNotification = licenseLimitNotification;

        this.requestLicenseUrl = ko.pureComputed(() =>
            license.generateLicenseRequestUrl(licenseLimitDetails.extractLimitType(licenseLimitNotification))
        );

        this.licenseRequestType = ko.pureComputed(() => {
            const licenseStatus = license.licenseStatus();
            if (!licenseStatus || licenseStatus.Type === "None") {
                return "Request";
            }

            return "Upgrade";
        });
    }
    
    private static extractLimitType(notification: alert | recentLicenseLimitError) {
        if (notification instanceof alert && notification.alertType() === "LicenseManager_LicenseLimit") {
            const limit = notification.details() as Raven.Server.NotificationCenter.Notifications.Details.LicenseLimitWarning;
            return limit.Type;
        }
        
        if (notification instanceof recentLicenseLimitError) {
            return notification.licenseLimitType();
        }
        
        return null;
    }

    register(): boolean {
        this.close();
        registration.showRegistrationDialog(this.licenseStatus(), false, true);
        return true;
    }

    static supportsDetailsFor(notification: abstractNotification) {
        const isLicenceAlert = (notification instanceof alert) && notification.isLicenseAlert(); 
        const isRecentLicenseError = (notification instanceof recentLicenseLimitError);
        
        return isLicenceAlert || isRecentLicenseError;
    }

    static showDetailsFor(licenseError: recentLicenseLimitError | alert, center: notificationCenter) {
        return app.showBootstrapDialog(new licenseLimitDetails(licenseError, center));
    }
}

export = licenseLimitDetails;
