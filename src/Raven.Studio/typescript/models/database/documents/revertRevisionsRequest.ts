/// <reference path="../../../../typings/tsd.d.ts"/>

type timeMagnitude = "minutes" | "hours" | "days";

class revertRevisionsRequest {

    static defaultDateFormat = "DD/MM/YYYY HH:mm";
    static defaultWindowValue = 96;
    
    date = ko.observable<string>();
    windowValue = ko.observable<number>(); //TODO: do we need window here?
    windowMagnitude = ko.observable<timeMagnitude>("hours");

    pointInTimeFormatted: KnockoutComputed<string>;

    validationGroup = ko.validatedObservable({
        date: this.date,
        windowValue: this.windowValue,
        windowMagnitude: this.windowMagnitude
    });
    
    constructor() {
        this.initObservables();
    }
    
    private initObservables() {
        this.pointInTimeFormatted = ko.pureComputed(() => {
            const date = this.date();
            if (date) {
                return moment(date, revertRevisionsRequest.defaultDateFormat).utc().format(revertRevisionsRequest.defaultDateFormat);
            }
            return null;
        });
        
        this.date.extend({
            required: true
        })
    }
    
    toDto(): Raven.Server.Documents.Revisions.RevertRevisionsRequest {
        let window = this.windowValue() || revertRevisionsRequest.defaultWindowValue;
        
        switch (this.windowMagnitude()) {
            case "minutes":
                window *= 60;
                break;
            case "hours":
                window *= 3600;
                break;
            case "days":
                window *= 24 * 3600;
                break;
        }
        
        const date = moment(this.date(), revertRevisionsRequest.defaultDateFormat).utc().toISOString();
        
        return {
            Time: date,
            WindowInSec: window
        }
    }
}

export = revertRevisionsRequest;
