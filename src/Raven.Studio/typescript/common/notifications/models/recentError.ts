/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");

class recentError extends abstractNotification {

    static currentErrorId = 1;

    details = ko.observable<string>();
    httpStatus = ko.observable<string>();

    shortMessage: KnockoutComputed<string>;

    constructor(dto: recentErrorDto) {
        super(null, dto);

        this.initObservables();
        this.updateWith(dto);
        this.createdAt(moment.utc());
    }

    updateWith(incomingChanges: recentErrorDto) {
        super.updateWith(incomingChanges);

        this.details(incomingChanges.Details);
        this.httpStatus(incomingChanges.HttpStatus);
        this.severity(incomingChanges.Severity);
    }

    private initObservables() {
        this.hasDetails = ko.pureComputed(() => !!this.details());
        this.shortMessage = ko.pureComputed(() => recentError.trimMessage(this.message()));
    }

    static tryExtractMessageAndException(details: string): { message: string, error: string } {
        try {
            const parsedDetails = JSON.parse(details);

            if (parsedDetails && parsedDetails.Message && parsedDetails.Error) {
                return {
                    message: parsedDetails.Message,
                    error: parsedDetails.Error
                };
            } else {
                return { message: details, error: null };
            }
        } catch (e) {
            return { message: details, error: null };
        }
    }

    static trimMessage(message: string) {
        if (!message) {
            return message;
        }
        const lineBreakIdx = Math.min(message.indexOf("\r"), message.indexOf("\r"));
        if (lineBreakIdx !== -1 && lineBreakIdx < 256) {
            return message.substr(0, lineBreakIdx);
        }

        if (message.length < 256) {
            return message;
        }

        return message.substr(0, 256) + "...";
    }

    static create(severity: Raven.Server.NotificationCenter.Notifications.NotificationSeverity, title: string, details: string, httpStatus: string) {
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
            Severity: severity
        } as recentErrorDto;

        return new recentError(dto);
    }
}

export = recentError;
