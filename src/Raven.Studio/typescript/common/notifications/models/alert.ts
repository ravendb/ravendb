/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import resource = require("models/resources/resource");

class alert extends abstractNotification {

    alertType = ko.observable<Raven.Server.NotificationCenter.Notifications.AlertType>();
    key = ko.observable<string>();
    severity = ko.observable<Raven.Server.NotificationCenter.Notifications.AlertSeverity>();
    details = ko.observable<Raven.Server.NotificationCenter.Notifications.Details.INotificationDetails>();

    isError: KnockoutObservable<boolean>;
    isWarning: KnockoutObservable<boolean>;
    isInfo: KnockoutObservable<boolean>;

    constructor(resource: resource, dto: Raven.Server.NotificationCenter.Notifications.AlertRaised) {
        super(resource, dto);
        this.updateWith(dto);

        this.initStatus();
        this.hasDetails = ko.pureComputed(() => !!this.details());
    }

    private initStatus() {
        this.isError = ko.pureComputed(() => this.severity() === "Error");
        this.isWarning = ko.pureComputed(() => this.severity() === "Warning");
        this.isInfo = ko.pureComputed(() => this.severity() === "Info");
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.AlertRaised) {
        super.updateWith(incomingChanges);

        this.alertType(incomingChanges.AlertType);
        this.key(incomingChanges.Key);
        this.details(incomingChanges.Details);
        this.severity(incomingChanges.Severity);
    }
}

export = alert;
