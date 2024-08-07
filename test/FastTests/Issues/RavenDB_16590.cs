// -----------------------------------------------------------------------
//  <copyright file="RavenDB_16590.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Raven.Server.Config;
using Raven.Server.Config.Attributes;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_16590 : NoDisposalNeeded
    {
        public RavenDB_16590(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void StudioListsOfIndexScopedSettings()
        {
            var perIndexSettings =
                RavenConfiguration.AllConfigurationEntriesForConfigurationNamesAndDebug.Value
                    .Where(x => x.Scope == ConfigurationEntryScope.ServerWideOrPerDatabaseOrPerIndex)
                    .SelectMany(x => x.Keys)
                    .OrderBy(x => x)
                    .ToList();

            var propertiesDeclaredInStudio = new List<string>
            {
                /*
                 * __        ___    ____  _   _ ___ _   _  ____ 
                 * \ \      / / \  |  _ \| \ | |_ _| \ | |/ ___|
                 *  \ \ /\ / / _ \ | |_) |  \| || ||  \| | |  _ 
                 *   \ V  V / ___ \|  _ <| |\  || || |\  | |_| |
                 *    \_/\_/_/   \_\_| \_\_| \_|___|_| \_|\____|
                 *
                 * Please synchronize this list with:
                 * src/Raven.Studio/typescript/models/database/index/configurationItem.ts
                 * field: PerDatabaseIndexingConfigurationOptions
                 *
                 * See for details: RavenDB-16590
                 * 
                 */
                "Indexing.AllowStringCompilation",
                "Indexing.Analyzers.Default",
                "Indexing.Analyzers.Exact.Default",
                "Indexing.Lucene.Analyzers.NGram.MaxGram",
                "Indexing.Lucene.Analyzers.NGram.MinGram",
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
                "Indexing.TimeSinceLastQueryAfterWhichDeepCleanupCanBeExecutedInMin",
                "Indexing.Throttling.TimeIntervalInMs",
                "Indexing.TransactionSizeLimitInMb",
                "Indexing.OrderByScoreAutomaticallyWhenBoostingIsInvolved",
                "Indexing.Lucene.UseCompoundFileInMerging",
                "Indexing.Lucene.IndexInputType",
                "Indexing.MaxTimeToWaitAfterFlushAndSyncWhenReplacingSideBySideIndexInSec",
                "Indexing.MinimumTotalSizeOfJournalsToRunFlushAndSyncWhenReplacingSideBySideIndexInMb",
                "Indexing.OrderByTicksAutomaticallyWhenDatesAreInvolved",
                "Query.RegexTimeoutInMs",
                "Indexing.Lucene.ReaderTermsIndexDivisor",
                "Indexing.Corax.DocumentsLimitForCompressionDictionaryCreation",
                "Indexing.Corax.IncludeDocumentScore",
                "Indexing.Corax.IncludeSpatialDistance",
                "Indexing.Corax.MaxMemoizationSizeInMb",
                "Indexing.Corax.MaxAllocationsAtDictionaryTrainingInMb",
                "Indexing.Corax.Static.ComplexFieldIndexingBehavior",

                //Obsolete studio keys:
                "Indexing.Static.SearchEngineType",
                "Indexing.Analyzers.NGram.MaxGram",
                "Indexing.Analyzers.NGram.MinGram",
                "Indexing.LargeSegmentSizeToMergeInMb",
                "Indexing.MaximumSizePerSegmentInMb",
                "Indexing.MaxTimeForMergesToKeepRunningInSec",
                "Indexing.MergeFactor",
                "Indexing.NumberOfLargeSegmentsToMergeInSingleBatch",
                "Indexing.UseCompoundFileInMerging",
            };

            var sortedStudioList = propertiesDeclaredInStudio.OrderBy(x => x).ToList();

             Assert.Equal(perIndexSettings, sortedStudioList);
        }
    }
}
