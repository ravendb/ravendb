/// <reference path="../../../../typings/tsd.d.ts"/>

import configuration = require("configuration");

class configurationItem {
    static readonly ConfigurationOptions = [
        configuration.indexing.maxTimeForDocumentTransactionToRemainOpen,
        configuration.indexing.minNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory,
        configuration.indexing.mapTimeout,
        configuration.indexing.mapTimeoutAfterEtagReached
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