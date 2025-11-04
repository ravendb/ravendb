/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import database = require("models/resources/database");
import licenseAgpl = require("viewmodels/common/notificationCenter/customControlls/licenseAgpl");

class alert extends abstractNotification {

    alertReason = ko.observable<Raven.Server.NotificationCenter.Notifications.AlertReason>();
    key = ko.observable<string>();
    details = ko.observable<Raven.Server.NotificationCenter.Notifications.Details.INotificationDetails>();
    isLicenseAlert: KnockoutComputed<boolean>;

    constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.AlertRaised) {
        super(db, dto);
        this.updateWith(dto);
        
        this.canBeDismissed = ko.pureComputed(() => (this.alertReason() !== "LicenseManager_AGPL3" && !this.readOnly) || !this.isPersistent());

        this.hasDetails = ko.pureComputed(() => !!this.details());
        
        this.isLicenseAlert = ko.pureComputed(() => {
            return this.alertReason().startsWith("LicenseManager") &&
                (this.alertReason() === "LicenseManager_LicenseLimit" || this.alertReason() === "LicenseManager_AGPL3");
        });

        this.canBePostponed = ko.pureComputed(() => this.isPersistent() && !this.isLicenseAlert() && !this.readOnly);
        
        this.injectCustomControl();
    }
    
    private injectCustomControl() {
        if (this.alertReason() === "LicenseManager_AGPL3") {
            this.customControl(new licenseAgpl());
        }
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.AlertRaised) {
        super.updateWith(incomingChanges);

        this.alertReason(incomingChanges.Reason);
        this.key(incomingChanges.Key);
        this.details(incomingChanges.Details);

        this.severity(incomingChanges.Severity);
    }
}

export = alert;
