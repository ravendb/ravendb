/// <reference path="../../../../typings/tsd.d.ts"/>
import jsonUtil = require("common/jsonUtil");

class configurationItem {
    static readonly PerDatabaseIndexingConfigurationOptions: Array<string> = [
        "Indexing.AllowStringCompilation",
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
        "Indexing.Lucene.IndexInputType",
        "Indexing.MaxTimeToWaitAfterFlushAndSyncWhenReplacingSideBySideIndexInSec",
        "Indexing.MinimumTotalSizeOfJournalsToRunFlushAndSyncWhenReplacingSideBySideIndexInMb",
        "Indexing.OrderByTicksAutomaticallyWhenDatesAreInvolved",
        "Query.RegexTimeoutInMs",
        "Indexing.Lucene.ReaderTermsIndexDivisor",
        "Indexing.Corax.IncludeDocumentScore",
        "Indexing.Corax.IncludeSpatialDistance",
        "Indexing.Corax.MaxMemoizationSizeInMb",
        "Indexing.Corax.MaxAllocationsAtDictionaryTrainingInMb",
        "Indexing.Corax.Static.ComplexFieldIndexingBehavior"

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

    unknownKey: KnockoutComputed<boolean>;

    validationGroup: KnockoutObservable<any>;
    dirtyFlag: () => DirtyFlag;

    constructor(key: string, value: string) {
        this.key(key);
        this.value(value);

        this.unknownKey = ko.pureComputed(() => {
            const key = this.key();
            if (!key) {
                return false;
            }
            return configurationItem.PerDatabaseIndexingConfigurationOptions.indexOf(key) === -1;
        });
        
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
