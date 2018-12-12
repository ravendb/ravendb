/// <reference path="../../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import utils = require("widgets/virtualGrid/virtualGridUtils");

abstract class abstractNotification {

    database: database;

    id: string;
    createdAt = ko.observable<moment.Moment>();
    isPersistent = ko.observable<boolean>();
    message = ko.observable<string>();
    title = ko.observable<string>();
    severity = ko.observable<Raven.Server.NotificationCenter.Notifications.NotificationSeverity>();
    type: Raven.Server.NotificationCenter.Notifications.NotificationType | virtualNotificationType;

    hasDetails: KnockoutComputed<boolean>;
    canBePostponed: KnockoutComputed<boolean>;
    canBeDismissed = ko.observable<boolean>(true);
    
    customControl = ko.observable<any>();

    displayDate: KnockoutComputed<moment.Moment>;

    headerClass: KnockoutComputed<string>;
    headerIconClass: KnockoutComputed<string>;
    cssClass: KnockoutComputed<string>;

    constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.database = db;
        this.id = dto.Id;
        this.type = dto.Type;

        this.headerClass = ko.pureComputed(() => {
            const severity = this.severity();

            switch (severity) {
                case "Error":
                    return "text-danger";
                case "Warning":
                    return "text-warning";
                case "Success":
                    return "text-success";
                case "Info":
                    return "text-info";
                default:
                    return "";
            }
        });
        
        this.headerIconClass = ko.pureComputed(() => this.database ? "icon-database-cutout" : "icon-global-cutout");

        this.cssClass = ko.pureComputed(() => {
            const severity = this.severity();

            switch (severity) {
                case "Error":
                    return "notification-danger";
                case "Warning":
                    return "notification-warning";
                case "Success":
                    return "notification-success";
                case "Info":
                    return "notification-info";
                default:
                    return "";
            }
        });

        this.canBePostponed = ko.pureComputed(() => this.isPersistent());

        this.displayDate = ko.pureComputed(() => moment(this.createdAt()).local());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.createdAt(incomingChanges.CreatedAt ? moment.utc(incomingChanges.CreatedAt) : null);
        this.isPersistent(incomingChanges.IsPersistent);

        if (incomingChanges.Message) {
            const escapedMessage = utils.escape(incomingChanges.Message);
            this.message(utils.nl2br(escapedMessage));    
        } else {
            this.message("");
        }
        
        this.title(incomingChanges.Title);
        this.severity(incomingChanges.Severity);
    }

}

export = abstractNotification;
