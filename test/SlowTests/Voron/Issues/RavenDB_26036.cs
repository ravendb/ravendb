using System;
using System.IO;
using System.Linq;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_26036 : StorageTest
{
    public RavenDB_26036(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        base.Configure(options);

        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = 10 * Constants.Storage.PageSize;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Must_not_remove_journal_referenced_by_peeked_record_during_flush()
    {
        var bytes = new byte[Constants.Storage.PageSize / 2];
        var random = new Random(42);
        random.NextBytes(bytes);

        for (int i = 0; i < 5; i++)
        {
            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("test");
                tree.Add($"items/{i}", new MemoryStream(bytes));
                tx.Commit();
            }
        }

        using (var tx = Env.WriteTransaction())
        {
            var tree = tx.CreateTree("test");

            var bigBytes = new byte[Constants.Storage.PageSize * 2];
            random.NextBytes(bigBytes);

            tree.Add("items/big", new MemoryStream(bigBytes));
            tx.Commit(); // this is going to fill up the first journal file completely
        }

        Assert.True(Env.Journal.Files.Count == 1,$"Expected exactly 1 journal file after writes, got {Env.Journal.Files.Count}");


        Assert.True(Env.Journal.Files.First().DoneWriting.IsRaised(), "Expected the journal file to have DoneWriting raised - meaning that there is exactly 0 pages available to write there");

        // Open a read transaction. Its ID = latest committed tx ID.
        // This constrains uptoTxIdExclusive during flush, causing the LAST record
        // in the queue to be peeked (txId >= uptoTxIdExclusive) rather than consumed.
        using (Env.ReadTransaction())
        {
            // Flush #1: Consumes records up to readTx.Id - 1, peeks the last record.
            // Without the fix: else branch in UpdateJournalStateUnderWriteTransactionLock removes it from _files.
            Env.FlushLogToDataFile();
        }

        // Read tx is disposed. uptoTxIdExclusive advances.
        // Flush #2: Consumes the previously peeked record (FlushedToJournal=X).
        // Without the fix: journal X was removed by Flush #1 -> "Unable to find journal file 0"
        Env.FlushLogToDataFile();
    }
}
