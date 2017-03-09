/// <reference path="../../../../typings/tsd.d.ts" />

import EVENTS = require("common/constants/events");
import database = require("models/resources/database");

abstract class abstractNotification {

    database: database;

    id: string;
    createdAt = ko.observable<moment.Moment>();
    isPersistent = ko.observable<boolean>();
    message = ko.observable<string>();
    title = ko.observable<string>();
    severity = ko.observable<Raven.Server.NotificationCenter.Notifications.NotificationSeverity>();
    type: Raven.Server.NotificationCenter.Notifications.NotificationType;
    hasDetails: KnockoutComputed<boolean>;
    canBePostponed: KnockoutComputed<boolean>;

    cssClass: KnockoutComputed<string>;

    constructor(db: database, dto: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.database = db;
        this.id = dto.Id;
        this.type = dto.Type;

        this.cssClass = ko.pureComputed(() => {
            const severity = this.severity();

            switch (severity) {
                case "Error":
                    return "panel-danger";
                case "Warning":
                    return "panel-warning";
                case "Success":
                    return "panel-success";
                case "Info":
                    return "panel-info";
                default:
                    return "";
            }
        });

        this.canBePostponed = ko.pureComputed(() => this.isPersistent());
    }

    updateWith(incomingChanges: Raven.Server.NotificationCenter.Notifications.Notification) {
        this.createdAt(incomingChanges.CreatedAt ? moment.utc(incomingChanges.CreatedAt) : null);
        this.isPersistent(incomingChanges.IsPersistent);
        this.message(incomingChanges.Message);
        this.title(incomingChanges.Title);
        this.severity(incomingChanges.Severity);
    }

}

export = abstractNotification;
