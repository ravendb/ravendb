/// <reference path="../../../../typings/tsd.d.ts"/>

class alert {
    alertLevel: string;
    createdAt: string;
    exception: string;
    message: string;
    title: string;
    uniqueKey: string;
    observed = ko.observable(false);
    lastDismissedAt: string;
    global: boolean;

    isVisible: KnockoutComputed<boolean>;
    createdAtHumanized: KnockoutComputed<string>;

    constructor(dto: alertDto) {
        this.alertLevel = dto.AlertLevel;
        this.createdAt = dto.CreatedAt;
        this.exception = dto.Exception;
        this.message = dto.Message;
        this.observed(dto.Observed);
        this.lastDismissedAt = dto.LastDismissedAt;
        this.title = dto.Title;
        this.uniqueKey = dto.UniqueKey;
    }

    toDto(): alertDto {
        return {
            AlertLevel: this.alertLevel,
            CreatedAt: this.createdAt,
            Exception: this.exception,
            Message: this.message,
            Observed: this.observed(),
            LastDismissedAt: this.lastDismissedAt,
            Title: this.title,
            UniqueKey: this.uniqueKey
        };
    }
}

export = alert;
