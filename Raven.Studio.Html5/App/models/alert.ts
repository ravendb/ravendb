class alert {
    alertLevel: string;
    createdAt: string;
    exception: string;
    message: string;
    title: string;
    uniqueKey: string;
    observed = ko.observable(false);

    isVisible: KnockoutComputed<boolean>;
    createdAtHumanized: KnockoutComputed<string>;

    constructor(dto: alertDto) {
        this.alertLevel = dto.AlertLevel;
        this.createdAt = dto.CreatedAt;
        this.exception = dto.Exception;
        this.message = dto.Message;
        this.observed(dto.Observed);
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
            Title: this.title,
            UniqueKey: this.uniqueKey
        };
    }
}

export = alert;