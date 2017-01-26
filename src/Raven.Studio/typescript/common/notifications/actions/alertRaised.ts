/// <reference path="../../../../typings/tsd.d.ts" />

import abstractAction = require("common/notifications/actions/abstractAction");

class alertRaised extends abstractAction {

    alertType = ko.observable<Raven.Server.NotificationCenter.Alerts.AlertType>();
    key = ko.observable<string>();
    severity = ko.observable<Raven.Server.NotificationCenter.Alerts.AlertSeverity>();

    isError: KnockoutObservable<boolean>;
    isWarning: KnockoutObservable<boolean>;
    isInfo: KnockoutObservable<boolean>;

    constructor(dto: Raven.Server.NotificationCenter.Actions.AlertRaised) {
        super(dto);
        this.updateWith(dto);

        this.initStatus();
    }

    private initStatus() {
        this.isError = ko.pureComputed(() => this.severity() === "Error");
        this.isWarning = ko.pureComputed(() => this.severity() === "Warning");
        this.isInfo = ko.pureComputed(() => this.severity() === "Info");
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Actions.AlertRaised) {
        super.updateWith(incomingChanges);

        this.alertType(incomingChanges.AlertType);
        this.key(incomingChanges.Key);
        this.severity(incomingChanges.Severity);
    }

     /* TODO
    id: string;
    key: string;
    read: boolean;
    content: Raven.Server.Alerts.IAlertContent;

    isOpen = ko.observable<boolean>(false);

    hasDetails() {
        return ["NewServerVersionAvailable"].some(x => x === this.type);
    }

    openDetails() {
        if (!this.isOpen()) {
            ko.postbox.publish("Alerts.DetailsOpen", this);
            this.isOpen(true);
        }
    }

    canOpenDetails() {
        return this.type !== 'NewServerVersionAvailable';
    }

    canBeDismissed() {
        return this.type !== 'NewServerVersionAvailable';
    }*/

}

export = alertRaised;
