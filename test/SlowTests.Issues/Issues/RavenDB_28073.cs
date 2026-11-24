using FastTests.Voron;
using Sparrow.Server.Meters;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_28073(ITestOutputHelper output) : StorageTest(output)
{
    private const long MandatoryThreshold = 512 * Constants.Size.Kilobyte;

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);

        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxUnsyncedBytesBeforeSync = MandatoryThreshold / 2;
        options.MaxUnsyncedBytesBeforeMandatorySync = MandatoryThreshold;
        options.SyncJournalsCountThreshold = int.MaxValue;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void RewritingTheSamePagesOnEveryFlushDoesNotForceASync()
    {
        // every flush writes at least a page, so the flushes add up to more than the mandatory threshold
        for (int round = 0; round <= 2 * MandatoryThreshold / Constants.Storage.PageSize; round++)
            WriteAndFlush("docs", firstKey: 0, keys: 16);

        Assert.Equal(IoMetrics.SyncReason.None, Env.WriteFlow.GetSyncReason(Env));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void WritingDistinctPagesForcesASync()
    {
        for (int round = 0; Env.Journal.Applicator.TotalWrittenButUnsyncedBytes <= MandatoryThreshold; round++)
        {
            Assert.True(round < 1024, "distinct pages were not counted as unsynced");
            WriteAndFlush("bulk", firstKey: round * 64, keys: 64);
        }

        Assert.Equal(IoMetrics.SyncReason.Mandatory, Env.WriteFlow.GetSyncReason(Env));
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PageRewrittenAfterSyncIsUnsyncedAgain()
    {
        WriteAndFlush("docs", firstKey: 0, keys: 16);

        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
            Assert.True(sync.SyncDataFile());

        Assert.False(Env.Journal.Applicator.ShouldSync);

        WriteAndFlush("docs", firstKey: 0, keys: 16);

        Assert.True(Env.Journal.Applicator.ShouldSync);
    }

    private void WriteAndFlush(string treeName, int firstKey, int keys)
    {
        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree(treeName);
            for (int i = firstKey; i < firstKey + keys; i++)
                tree.Add($"items/{i:D10}", new byte[1024]);
            tx.Commit();
        }

        Env.FlushLogToDataFile();
    }
}
