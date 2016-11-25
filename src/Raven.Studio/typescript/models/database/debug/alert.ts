/// <reference path="../../../../typings/tsd.d.ts"/>

class alert {

    id: string;
    key: string;
    message: KnockoutObservable<string>;
    read: boolean;
    severity: KnockoutObservable<Raven.Server.Alerts.AlertSeverity>;
    type: Raven.Server.Alerts.AlertType;
    createdAt: string;
    dismissedUntil: string;
    content: Raven.Server.Alerts.IAlertContent;

    global: boolean;

    isError: KnockoutObservable<boolean>;
    isWarning: KnockoutObservable<boolean>;
    isInfo: KnockoutObservable<boolean>;

    isOpen = ko.observable<boolean>(false);

    constructor(dto: Raven.Server.Alerts.Alert) {
        this.id = dto.Id;
        this.key = dto.Key;
        this.message = ko.observable(dto.Message);
        this.read = dto.Read;
        this.severity = ko.observable(dto.Severity);
        this.type = dto.Type;
        this.createdAt = dto.CreatedAt;
        this.dismissedUntil = dto.DismissedUntil;
        this.content = dto.Content;

        this.initStatus();
    }

    private initStatus() {
        this.isError = ko.pureComputed(() =>
            this.severity() === ("Error" as Raven.Server.Alerts.AlertSeverity));
        this.isWarning = ko.pureComputed(() =>
            this.severity() === ("Warning" as Raven.Server.Alerts.AlertSeverity));
        this.isInfo = ko.pureComputed(() =>
            this.severity() === ("Info" as Raven.Server.Alerts.AlertSeverity));
    }

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
    }

}

export = alert;
