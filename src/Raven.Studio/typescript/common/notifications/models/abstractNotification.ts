/// <reference path="../../../../typings/tsd.d.ts" />
import database = require("models/resources/database");
import generalUtils = require("common/generalUtils");
import accessManager = require("common/shell/accessManager");
import moment = require("moment");

abstract class abstractNotification {

    databaseName: string;

    id: string;
    readOnly: boolean;
    createdAt = ko.observable<moment.Moment>();
    isPersistent = ko.observable<boolean>();
    message = ko.observable<string>();
    title = ko.observable<string>();
    severity = ko.observable<Raven.Server.NotificationCenter.Notifications.NotificationSeverity>();
    type: Raven.Server.NotificationCenter.Notifications.NotificationType | virtualNotificationType;
    
    hasDetails: KnockoutComputed<boolean>;
    
    canBePostponed: KnockoutComputed<boolean>;
    canBeDismissed = ko.pureComputed(() => !(this.readOnly && this.isPersistent()));
    requiresRemoteDismiss = ko.observable(true);
    
    customControl = ko.observable<any>();

    displayDate: KnockoutComputed<moment.Moment>;

    headerClass: KnockoutComputed<string>;
    headerIconClass: KnockoutComputed<string>;
    cssClass: KnockoutComputed<string>;

    protected constructor(db: database | string, dto: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.databaseName = typeof db === "string" ? db : db?.name;
        this.id = dto.Id;
        this.type = dto.Type;
        this.readOnly = !accessManager.canHandleOperation(db ? "DatabaseReadWrite" : "Operator", this.databaseName);

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
        
        this.headerIconClass = ko.pureComputed(() => this.databaseName ? "icon-database-cutout" : "icon-global-cutout");

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

        this.canBePostponed = ko.pureComputed(() => this.isPersistent() && !this.readOnly);

        this.displayDate = ko.pureComputed(() => moment(this.createdAt()).local());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.createdAt(incomingChanges.CreatedAt ? moment.utc(incomingChanges.CreatedAt) : null);
        this.isPersistent(incomingChanges.IsPersistent);

        if (incomingChanges.Message) {
            const escapedMessage = generalUtils.escapeHtml(incomingChanges.Message);
            this.message(generalUtils.nl2br(escapedMessage));    
        } else {
            this.message("");
        }
        
        this.title(incomingChanges.Title);
        this.severity(incomingChanges.Severity);
    }

}

export = abstractNotification;
