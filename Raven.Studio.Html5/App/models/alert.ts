class alert {
    alertLevel: string;
    createdAt: string;
    exception: string;
    message: string;
    observed = ko.observable(false);
    title: string;
    uniqueKey: string;

    constructor(dto: alertDto) {
        this.alertLevel = dto.AlertLevel;
        this.createdAt = dto.CreatedAt;
        this.exception = dto.Exception;
        this.message = dto.Message;
        this.observed(dto.Observed);
        this.title = dto.Title;
        this.uniqueKey = dto.UniqueKey;
    }
}

export = alert;