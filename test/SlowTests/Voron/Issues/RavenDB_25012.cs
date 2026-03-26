using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron;
using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_25012: StorageTest
{
    public RavenDB_25012(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void OldestTransactionMustNotBeZeroWhileAnyTransactionIsActive_EvenDuringCompactionRace()
    {
        // Expand internal bag to at least growthFactor*4 elements (ActiveTransactions uses growthFactor=64)
        const int expandTo = 256; // 64 * 4

        for (int i = 0; i < 3; i++)
        {
            var txs = new List<Transaction>(expandTo);
            for (int j = 0; j < expandTo; j++)
                txs.Add(Env.ReadTransaction());
            foreach (var t in txs)
                t.Dispose();
        }

        // Try multiple times to hit the race window between Remove() compaction and Add()
        var sw = System.Diagnostics.Stopwatch.StartNew();
        var deadline = TimeSpan.FromSeconds(3);

        while (sw.Elapsed < deadline)
        {
            var disposerDone = new ManualResetEventSlim(false);
            var haveActive = new ManualResetEventSlim(false);
            var keepActive = new ManualResetEventSlim(false);
            long createdTxId = 0;

            // 1. Create a batch of transactions we will dispose to trigger compaction
            var batch = new List<Transaction>(expandTo);
            for (int j = 0; j < expandTo; j++)
                batch.Add(Env.ReadTransaction());

            // 2. Start a task that will dispose them all (last dispose may trigger compaction)
            var disposer = Task.Run(() =>
            {
                foreach (var t in batch)
                    t.Dispose();
                disposerDone.Set();
            });

            // 3. In parallel, start a task that tries to create a new transaction during compaction window
            var racer = Task.Run(() =>
            {
                // Spin until disposer is almost done, then try to create a tx repeatedly
                while (!disposerDone.IsSet)
                {
                    // busy wait a little to reduce overhead
                    Thread.SpinWait(100);
                }

                // try several times quickly to land inside the compaction window
                for (int k = 0; k < 100; k++)
                {
                    using (var tx = Env.ReadTransaction())
                    {
                        createdTxId = tx.LowLevelTransaction.Id;
                        haveActive.Set();

                        // ensure tx stays alive until the main thread signals it's done sampling
                        keepActive.Wait();
                    }

                    if (keepActive.IsSet)
                        break;
                }
            });

            // 4. While the racer tries to open a new tx, sample OldestTransaction
            //    as soon as we know there is an active tx (haveActive), assert oldest != 0
            bool observedActive = false;
            var sampleUntil = DateTime.UtcNow + TimeSpan.FromMilliseconds(200);

            try
            {
                while (DateTime.UtcNow < sampleUntil)
                {
                    // let flusher recalculate using ScanOldest
                    Env.ActiveTransactions.ForceRecheckingOldestTransactionByFlusherThread();

                    if (haveActive.IsSet)
                    {
                        observedActive = true;
                        var oldest = Env.ActiveTransactions.OldestTransaction;
                        Assert.True(oldest != 0, $"Observed OldestTransaction=0 while a transaction (id {createdTxId}) was active");

                        // allow the racer to dispose the tx now
                        keepActive.Set();
                        break;
                    }

                    Thread.SpinWait(200);
                }
            }
            finally
            {
                // Always unblock racer just in case we exit without seeing haveActive
                if (!keepActive.IsSet)
                    keepActive.Set();
            }
            
            Task.WaitAll(new[] { disposer, racer }, TimeSpan.FromSeconds(1));

            // If we didn't manage to observe the active window, try again
            if (!observedActive)
                continue;

            // One successful observation is enough for the test
            break;
        }

        // After all, there should be no leftovers and oldest should be 0
        Env.ActiveTransactions.ForceRecheckingOldestTransactionByFlusherThread();
        Assert.Equal(0, Env.ActiveTransactions.OldestTransaction);
    }
}
