/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");
import configuration = require("configuration");

class configurationItem {

    static readonly PerDatabaseIndexingConfigurationOptions: Array<string> = [
        "Indexing.Analyzers.Default",
        "Indexing.Analyzers.Exact.Default",
        "Indexing.Analyzers.NGram.MaxGram",
        "Indexing.Analyzers.NGram.MinGram",
        "Indexing.Analyzers.Search.Default",
        "Indexing.Encrypted.TransactionSizeLimitInMb",
        "Indexing.IndexEmptyEntries",
        "Indexing.IndexMissingFieldsAsNull",
        "Indexing.LargeSegmentSizeToMergeInMb",
        "Indexing.ManagedAllocationsBatchSizeLimitInMb",
        "Indexing.MapBatchSize",
        "Indexing.MapTimeoutAfterEtagReachedInMin",
        "Indexing.MapTimeoutInSec",
        "Indexing.MaximumSizePerSegmentInMb",
        "Indexing.MaxStepsForScript",
        "Indexing.MaxTimeForDocumentTransactionToRemainOpenInSec",
        "Indexing.MaxTimeForMergesToKeepRunningInSec",
        "Indexing.MergeFactor",
        "Indexing.Metrics.Enabled",
        "Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory",
        "Indexing.NumberOfConcurrentStoppedBatchesIfRunningLowOnMemory",
        "Indexing.NumberOfLargeSegmentsToMergeInSingleBatch",
        "Indexing.ScratchSpaceLimitInMb",
        "Indexing.Throttling.TimeIntervalInMs",
        "Indexing.TransactionSizeLimitInMb"
    ];
    
    key = ko.observable<string>();
    value = ko.observable<string>();

    validationGroup: KnockoutObservable<any>;
    dirtyFlag: () => DirtyFlag;

    constructor(key: string, value: string) {
        this.key(key);
        this.value(value);

        this.initValidation();

        this.dirtyFlag = new ko.DirtyFlag([
            this.key, 
            this.value,
        ], false, jsonUtil.newLineNormalizingHashFunction);
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
