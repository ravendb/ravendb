/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class configurationItem {
    static readonly PerDatabaseIndexingConfigurationOptions: Array<string> = [
        "Indexing.Analyzers.Default",
        "Indexing.Analyzers.Exact.Default",
        "Indexing.Analyzers.Lucene.NGram.MaxGram",
        "Indexing.Analyzers.Lucene.NGram.MinGram",
        "Indexing.Analyzers.Search.Default",
        "Indexing.QueryClauseCache.Disabled",
        "Indexing.QueryClauseCache.RepeatedQueriesTimeFrameInSec",
        "Indexing.Encrypted.TransactionSizeLimitInMb",
        "Indexing.IndexEmptyEntries",
        "Indexing.IndexMissingFieldsAsNull",
        "Indexing.Lucene.LargeSegmentSizeToMergeInMb",
        "Indexing.ManagedAllocationsBatchSizeLimitInMb",
        "Indexing.MapBatchSize",
        "Indexing.MapTimeoutAfterEtagReachedInMin",
        "Indexing.MapTimeoutInSec",
        "Indexing.Lucene.MaximumSizePerSegmentInMb",
        "Indexing.MaxStepsForScript",
        "Indexing.MaxTimeForDocumentTransactionToRemainOpenInSec",
        "Indexing.Lucene.MaxTimeForMergesToKeepRunningInSec",
        "Indexing.Lucene.MergeFactor",
        "Indexing.Metrics.Enabled",
        "Indexing.MinNumberOfMapAttemptsAfterWhichBatchWillBeCanceledIfRunningLowOnMemory",
        "Indexing.NumberOfConcurrentStoppedBatchesIfRunningLowOnMemory",
        "Indexing.Lucene.NumberOfLargeSegmentsToMergeInSingleBatch",
        "Indexing.ScratchSpaceLimitInMb",
        "Indexing.Throttling.TimeIntervalInMs",
        "Indexing.TimeSinceLastQueryAfterWhichDeepCleanupCanBeExecutedInMin",
        "Indexing.TransactionSizeLimitInMb",
        "Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved",
        "Indexing.Lucene.UseCompoundFileInMerging",
        "Indexing.Lucene.IndexInputType"
        // "Indexing.Static.SearchEngineType" - ignoring as we have dedicated widget to set that
        /*
            Obsolete keys:
                "Indexing.Analyzers.NGram.MaxGram",
                "Indexing.Analyzers.NGram.MinGram",
                "Indexing.LargeSegmentSizeToMergeInMb",
                "Indexing.MaximumSizePerSegmentInMb",
                "Indexing.MaxTimeForMergesToKeepRunningInSec",
                "Indexing.MergeFactor",
                "Indexing.NumberOfLargeSegmentsToMergeInSingleBatch",
         */
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
