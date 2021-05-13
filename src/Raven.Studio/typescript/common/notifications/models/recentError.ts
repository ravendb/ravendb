/// <reference path="../../../../typings/tsd.d.ts" />

import abstractNotification = require("common/notifications/models/abstractNotification");
import generalUtils = require("common/generalUtils");

class recentError extends abstractNotification {

    static currentErrorId = 1;
    
    details = ko.observable<string>();
    httpStatus = ko.observable<string>();
    
    shortMessage: KnockoutComputed<string>;
    longerMessage: KnockoutComputed<string>;

    constructor(dto: recentErrorDto) {
        super(null, dto);
        
        this.requiresRemoteDismiss(false);

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
        this.shortMessage = ko.pureComputed(() => generalUtils.trimMessage(this.message(), 256));
        this.longerMessage = ko.pureComputed(() => generalUtils.trimMessage(this.message(), 1024));
        this.hasDetails = ko.pureComputed(() => !!this.details() || this.shortMessage() !== this.message());
    }

    static tryExtractMessageAndException(details: string): { message: string, error: string } {
        try {
            const parsedDetails = JSON.parse(details);

            if (parsedDetails && parsedDetails.Message) {
                return {
                    message: parsedDetails.Message,
                    error: parsedDetails.Error
                };
            }
        } catch (e) {
        }

        // fallback to message with entire details
        return { message: details, error: null };
    }
    
}

export = recentError;
