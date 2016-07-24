class logEntry {
    level = ko.observable<string>();
    timeStamp = ko.observable<string>();
    message = ko.observable<string>();
    exception = ko.observable<string>();
    loggerName = ko.observable<string>();
    database = ko.observable<string>();
    stackTrace = ko.observable<string>();
    dateMoment = ko.observable<moment.Moment>();
    dateRaw = ko.observable<number>();
    humanizedTimestamp: KnockoutComputed<string>;
    timeStampText: KnockoutComputed<string>;

    constructor(dto: logDto, private nowProvider: KnockoutObservable<moment.Moment>) {
        this.level(dto.Level);
        this.timeStamp(dto.TimeStamp);
        this.message(dto.Message);
        this.exception(dto.Exception);
        this.loggerName(dto.LoggerName);
        this.database(dto.Database);
        this.stackTrace(dto.StackTrace);

        // since we know date is in ISO format parse it to date and then pass to moment - it will skip format detection
        this.dateMoment(moment(new Date(this.timeStamp())));
        this.dateRaw(this.dateMoment().toDate().getTime());

        this.humanizedTimestamp = ko.pureComputed(() => {
            var dateMoment = this.dateMoment();
            var agoInMs = dateMoment.diff(this.nowProvider());
            var humanized = moment.duration(agoInMs).humanize(true);
            return humanized;
        });

        this.timeStampText = ko.pureComputed(() => {
            var dateMoment = this.dateMoment();
            return dateMoment.format(" (MM/DD/YY, h:mma)");
        });
    }

    copyFrom(newEntry: logEntry) {
        this.level(newEntry.level());
        this.timeStamp(newEntry.timeStamp());
        this.message(newEntry.message());
        this.exception(newEntry.exception());
        this.loggerName(newEntry.loggerName());
        this.database(newEntry.database());
        this.stackTrace(newEntry.stackTrace());
        this.dateMoment(moment(new Date(newEntry.timeStamp())));
        this.dateRaw(this.dateMoment().toDate().getTime());
    }
}

export = logEntry;
