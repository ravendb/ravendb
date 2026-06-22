using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class TimeSeriesCompactionTests : RavenTestBase
    {
        public TimeSeriesCompactionTests(ITestOutputHelper output) : base(output)
        {
        }

        // Regression test for a bug in StorageCompaction.CopyTableTree.
        //
        // TimeSeriesStats uses a global primary key (TimeSeriesStatsKey, IsGlobal=true).
        // Compaction therefore falls back to iterating via the first local secondary index,
        // which is PolicyIndex.  PolicyIndex is non-unique: every raw time-series entry
        // maps to the same key "rawpolicy\x1E".
        //
        // The compaction pagination checkpoint (lastSlice) starts as Slices.BeforeAllKeys.
        // The guard that prevents mid-group breaks reads:
        //
        //   if (lastSlice.Equals(tvr.Key) == false && transactionSize >= limit)
        //       break;
        //
        // Because BeforeAllKeys is never equal to any real key, this condition is always
        // true from the very first row.  (In fact lastSlice.Equals is reference/pointer
        // equality, so it stays true on every batch even after lastSlice holds a real key.)
        // The first time the transaction exceeds limit the
        // batch breaks mid-"rawpolicy\x1E" group.  On the next batch skip=1 is used, which
        // skips only one row instead of all already-committed rows, causing re-insertion
        // of already-copied entries and a:
        //
        //   VoronConcurrencyErrorException: Value already exists, but requested NewOnly
        //
        // Setting Storage.MaxScratchBufferSizeInMb to a small value forces the break to
        // occur with a tractable number of entries (limit = MaxScratchBufferSize/2).
        [RavenFact(RavenTestCategory.TimeSeries | RavenTestCategory.Voron)]
        public async Task Compaction_ManyRawTimeSeries_ShouldNotFailWithDuplicateKey()
        {
            // 1 MB scratch → limit = 512 KB ≈ 5 000 entries/batch at ~100 B/entry.
            // 10 000 entries ensures the first batch breaks mid-group.
            using (var store = GetDocumentStore(new Options
                   {
                       RunInMemory = false,
                       ModifyDatabaseRecord = record =>
                           record.Settings["Storage.MaxScratchBufferSizeInMb"] = "1"
                   }))
            {
                const int documentCount = 10_000;
                var baseline = new DateTime(2024, 1, 1, 0, 0, 0, DateTimeKind.Utc);

                // Write documentCount documents, each with one raw time-series append.
                // All stats entries land in PolicyIndex under the same key "rawpolicy\x1E".
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < documentCount; i++)
                    {
                        await bulk.StoreAsync(new { }, $"products/{i}");
                        using (var ts = bulk.TimeSeriesFor($"products/{i}", "Revenue"))
                            await ts.AppendAsync(baseline, new[] { (double)i });
                    }
                }

                var compactOperation = await store.Maintenance.Server.SendAsync(
                    new CompactDatabaseOperation(new CompactSettings
                    {
                        DatabaseName = store.Database,
                        Documents = true
                    }));

                // Before the fix this throws:
                //   InvalidOperationException: Failed to execute compaction
                //   ---> VoronConcurrencyErrorException: Value already exists, but requested NewOnly
                await compactOperation.WaitForCompletionAsync(TimeSpan.FromSeconds(120));

                // After the fix all data must be readable in the compacted database.
                using (var session = store.OpenSession())
                {
                    session.Advanced.MaxNumberOfRequestsPerSession = documentCount + 1;

                    for (int i = 0; i < documentCount; i++)
                    {
                        var entries = session.TimeSeriesFor($"products/{i}", "Revenue").Get();
                        Assert.NotNull(entries);
                        Assert.Equal(1, entries.Length);
                        Assert.Equal((double)i, entries[0].Values[0]);
                    }
                }
            }
        }
    }
}
