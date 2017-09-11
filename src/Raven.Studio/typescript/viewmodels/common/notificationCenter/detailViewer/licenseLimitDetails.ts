import app = require("durandal/app");
import abstractNotification = require("common/notifications/models/abstractNotification");
import notificationCenter = require("common/notifications/notificationCenter");
import recentError = require("common/notifications/models/recentError");
import dialogViewModelBase = require("viewmodels/dialogViewModelBase");
import alert = require("common/notifications/models/alert");
import license = require("models/auth/licenseModel");
import registration = require("viewmodels/shell/registration");

class licenseLimitDetails extends dialogViewModelBase {

    protected readonly licenseLimitNotification: recentError | alert;
    protected readonly dismissFunction: () => void;
    licenseStatus = license.licenseStatus;
    url: KnockoutComputed<string>;

    constructor(licenseLimitNotification: alert | recentError, notificationCenter: notificationCenter) {
        super();
        this.bindToCurrentInstance("close");

        this.licenseLimitNotification = licenseLimitNotification;

        this.url = ko.pureComputed(() => {
            let url = "https://ravendb.net/request-license?";
            const error = licenseLimitNotification as recentError;
            if (error && error.licenseLimitType()) {
                url += `limit=${error.licenseLimitType()}`;
            }
            const status = this.licenseStatus();
            if (status && status.Id) {
                url += `&id=${status.Id}`;
            }

            return url;
        });
    }

    register(): boolean {
        this.close();
        registration.showRegistrationDialog(this.licenseStatus(), false, true);
        return true;
    }

    static supportsDetailsFor(notification: abstractNotification) {
        const isLicenceAlert = (notification instanceof alert) && notification.isLicenseAlert(); 
        const isRecentLicenseError = (notification instanceof recentError) && notification.details() === recentError.licenceLimitMarker;
        
        return isLicenceAlert || isRecentLicenseError;
    }

    static showDetailsFor(licenseError: recentError | alert, center: notificationCenter) {
        return app.showBootstrapDialog(new licenseLimitDetails(licenseError, center));
    }
}

export = licenseLimitDetails;
