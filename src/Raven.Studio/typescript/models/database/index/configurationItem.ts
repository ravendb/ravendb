/// <reference path="../../../../typings/tsd.d.ts"/>

class configurationItem {
    static readonly ConfigurationOptions = [
        "Raven/Indexing/RunInMemory",
        "Raven/Indexing/MaxTimeForDocumentTransactionToRemainOpenInSec",
        "Raven/Indexing/MaxIndexOutputsPerDocument",
        "Raven/Indexing/MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory",
        "Raven/Indexing/MapTimeoutInSec",
        "Raven/Indexing/MapTimeoutAfterEtagReachedInMin"
    ];

    key = ko.observable<string>();
    value = ko.observable<string>();

    validationGroup: KnockoutObservable<any>;

    constructor(key: string, value: string) {
        this.key(key);
        this.value(value);

        this.initValidation();
    }

    private initValidation() {
        this.key.extend({
            required: true
        });

        this.value.extend({
            required: true
        });

        this.validationGroup = ko.validatedObservable({
            key: this.key,
            value: this.value
        });
    }

    static empty() {
        return new configurationItem("", "");
    }
}

export = configurationItem;