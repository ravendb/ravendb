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
                RavenConfiguration.AllConfigurationEntries.Value
                    .Where(x => x.Scope == ConfigurationEntryScope.ServerWideOrPerDatabaseOrPerIndex)
                    .SelectMany(x => x.Keys)
                    .OrderBy(x => x)
                    .ToList();

            var propertiesDeclaredInStudio = new List<string>
            {
                /*
                 *  __        ___    ____  _   _ ___ _   _  ____ 
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
            };

            var sortedStudioList = propertiesDeclaredInStudio.OrderBy(x => x).ToList();

            Assert.Equal(perIndexSettings, sortedStudioList);
        }
    }
}
