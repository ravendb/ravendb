using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Threading;
using FastTests;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27377(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int PipelineDepth = 4;

    [RavenFact(RavenTestCategory.Voron)]
    public void AsyncCommitChain_KeepsSeveralJournalWritesInFlight()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        var concurrent = 0;
        var maxConcurrent = 0;

        using (var env = new StorageEnvironment(CreateOptions(path)))
        {
            env.Options.ForTestingPurposesOnly().OnJournalWrite = (_, _) =>
            {
                var current = Interlocked.Increment(ref concurrent);
                InterlockedMax(ref maxConcurrent, current);
                Thread.Sleep(25);
                Interlocked.Decrement(ref concurrent);
            };

            RunAsyncCommitChain(env, 12, (tx, i) => tx.CreateTree("tree").Add($"items/{i:D4}", i.ToString()));

            env.Options.ForTestingPurposesOnly().OnJournalWrite = null;
        }

        Output.WriteLine($"highest number of concurrent journal writes: {maxConcurrent}");

        Assert.True(maxConcurrent > 1,
            $"journal writes were not pipelined - the highest number of concurrent journal writes was {maxConcurrent}");
        Assert.True(maxConcurrent <= PipelineDepth,
            $"the pipeline window was exceeded - {maxConcurrent} concurrent journal writes with a limit of {PipelineDepth}");
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void LostJournalWrite_IsNotPublishedEvenWhenTheNextWriteLands()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using var bothWritesAreInFlight = new ManualResetEventSlim(false);
        using var theNextWriteLanded = new ManualResetEventSlim(false);
        using var holdBackTheLostWrite = new ManualResetEventSlim(false);
        using var isTheLostWrite = new ThreadLocal<bool>();

        var sync = new object();
        var inFlight = new List<long>();
        long lostWritePosition = -1;

        long committedBeforeTheLostWrite;
        long readableAfterTheFailure;

        var env = new StorageEnvironment(CreateOptions(path));
        try
        {
            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("tree").Add("before", "durable");
                tx.Commit();
            }

            committedBeforeTheLostWrite = env.CurrentReadTransactionId;

            env.Options.ForTestingPurposesOnly().OnJournalWrite = (position, _) =>
            {
                lock (sync)
                {
                    inFlight.Add(position);
                    if (inFlight.Count == 2)
                    {
                        lostWritePosition = Math.Min(inFlight[0], inFlight[1]);
                        bothWritesAreInFlight.Set();
                    }
                }

                if (bothWritesAreInFlight.Wait(TimeSpan.FromMinutes(1)) == false)
                    return;

                isTheLostWrite.Value = position == Volatile.Read(ref lostWritePosition);

                if (isTheLostWrite.Value)
                    holdBackTheLostWrite.Wait(TimeSpan.FromMinutes(1));
            };
            env.Options.ForTestingPurposesOnly().OnJournalWriteCompleted = (position, _) =>
            {
                if (position != Volatile.Read(ref lostWritePosition))
                    theNextWriteLanded.Set();
            };
            env.Options.ForTestingPurposesOnly().SimulatePartialJournalWriteFailure = _ =>
                isTheLostWrite.Value
                    ? new StorageEnvironmentOptions.TestingStuff.PartialJournalWriteFailure
                    {
                        NumberOf4KbsToWrite = 0,
                        Error = new IOException("RavenDB-27377 simulated lost journal write")
                    }
                    : null;

            var context = new TransactionPersistentContext(true);
            var lost = env.WriteTransaction(context);
            lost.CreateTree("tree").Add("lost", "lost");

            var afterTheLostOne = lost.BeginAsyncCommitAndStartNewTransaction(context);
            afterTheLostOne.CreateTree("tree").Add("after-the-lost-one", "written-but-unreachable");

            var neverCommitted = afterTheLostOne.BeginAsyncCommitAndStartNewTransaction(context);

            Assert.True(theNextWriteLanded.Wait(TimeSpan.FromMinutes(1)),
                "the journal write submitted after the lost one never reached the disk");

            Assert.Equal(committedBeforeTheLostWrite, env.CurrentReadTransactionId);

            neverCommitted.Dispose();

            holdBackTheLostWrite.Set();

            Assert.ThrowsAny<Exception>(() => lost.EndAsyncCommit());
            Assert.ThrowsAny<Exception>(() => afterTheLostOne.EndAsyncCommit());

            Record.Exception(() => lost.Dispose());
            Record.Exception(() => afterTheLostOne.Dispose());

            Assert.True(WaitForCatastrophicFailure(env), "the environment did not fail after a journal write was lost");

            readableAfterTheFailure = env.CurrentReadTransactionId;
        }
        finally
        {
            Record.Exception(() => env.Dispose());
        }

        Assert.Equal(committedBeforeTheLostWrite, readableAfterTheFailure);

        using (var recovered = new StorageEnvironment(CreateOptions(path)))
        using (var tx = recovered.ReadTransaction())
        {
            var tree = tx.ReadTree("tree");
            Assert.NotNull(tree);
            Assert.NotNull(tree.Read("before"));
            Assert.Null(tree.Read("lost"));
            Assert.Null(tree.Read("after-the-lost-one"));
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ValidRecordAfterAHole_WhoseWatermarkClaimsTheHole_IsCorruption()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        var written = new List<string>();

        using (var env = new StorageEnvironment(CreateOptions(path, maxConcurrentJournalWrites: 1)))
        {
            for (int i = 0; i < 6; i++)
            {
                using var tx = env.WriteTransaction();
                var key = $"items/{i:D4}";
                tx.CreateTree("tree").Add(key, new string((char)('a' + i), 64));
                written.Add(key);
                tx.Commit();
            }
        }

        var holePosition = ZeroOutTransactionRecord(path, fromTheEnd: 1);
        Output.WriteLine($"punched a hole at offset {holePosition}");

        var options = CreateOptions(path);
        var recoveryErrors = 0;
        options.OnRecoveryError += (_, _) => Interlocked.Increment(ref recoveryErrors);

        var error = Record.Exception(() =>
        {
            using var env = new StorageEnvironment(options);

            using var tx = env.ReadTransaction();
            var tree = tx.ReadTree("tree");
            foreach (var key in written)
                Assert.NotNull(tree?.Read(key));
        });

        Output.WriteLine($"recovery error: {error?.Message ?? "none"}, recovery error callbacks: {recoveryErrors}");

        Assert.True(error != null || recoveryErrors > 0,
            "a valid transaction found after a hole whose watermark claims the missing transaction was already durable must not be silently discarded");
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void SerialJournalWrites_ArePublishedWhenCommitReturns()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        using (var env = new StorageEnvironment(CreateOptions(path, maxConcurrentJournalWrites: 1)))
        {
            for (int i = 0; i < 20; i++)
            {
                long id;
                using (var tx = env.WriteTransaction())
                {
                    tx.CreateTree("tree").Add($"items/{i:D4}", i.ToString());
                    id = tx.LowLevelTransaction.Id;
                    tx.Commit();
                }

                Assert.Equal(0, env.Journal.ForTestingPurposesOnly().InFlightJournalWrites);
                Assert.Equal(id, env.CurrentReadTransactionId);
            }
        }

        using (var env = new StorageEnvironment(CreateOptions(path, maxConcurrentJournalWrites: 1)))
        using (var tx = env.ReadTransaction())
        {
            var tree = tx.ReadTree("tree");
            for (int i = 0; i < 20; i++)
                Assert.NotNull(tree.Read($"items/{i:D4}"));
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void PipelinedCommits_SurviveARestart()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);

        const int transactions = 64;

        using (var env = new StorageEnvironment(CreateOptions(path)))
        {
            RunAsyncCommitChain(env, transactions, (tx, i) => tx.CreateTree("tree").Add($"items/{i:D4}", Value(i)));
        }

        using (var env = new StorageEnvironment(CreateOptions(path)))
        using (var tx = env.ReadTransaction())
        {
            var tree = tx.ReadTree("tree");
            Assert.NotNull(tree);
            for (int i = 0; i < transactions; i++)
            {
                var read = tree.Read($"items/{i:D4}");
                Assert.True(read != null, $"items/{i:D4} is missing after a restart");
                Assert.Equal(Value(i), read.Reader.ToStringValue());
            }
        }

        static string Value(int i) => i + "-" + new string((char)('a' + i % 26), 200);
    }

    private static void RunAsyncCommitChain(StorageEnvironment env, int transactions, Action<Transaction, int> work)
    {
        var context = new TransactionPersistentContext(true);
        var inFlight = new Queue<Transaction>();
        var previous = env.WriteTransaction(context);
        work(previous, 0);

        for (int i = 1; i < transactions; i++)
        {
            var current = previous.BeginAsyncCommitAndStartNewTransaction(context);
            inFlight.Enqueue(previous);

            work(current, i);

            while (inFlight.Count >= PipelineDepth)
                CompleteOldest(inFlight);

            previous = current;
        }

        while (inFlight.Count > 0)
            CompleteOldest(inFlight);

        previous.Commit();
        previous.Dispose();

        static void CompleteOldest(Queue<Transaction> inFlight)
        {
            var oldest = inFlight.Dequeue();
            oldest.EndAsyncCommit();
            oldest.Dispose();
        }
    }

    private static StorageEnvironmentOptions CreateOptions(string path, int maxConcurrentJournalWrites = PipelineDepth)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(path);
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxConcurrentJournalWrites = maxConcurrentJournalWrites;
        return options;
    }

    private static bool WaitForCatastrophicFailure(StorageEnvironment env)
    {
        var sp = Stopwatch.StartNew();
        while (sp.Elapsed < TimeSpan.FromMinutes(1))
        {
            try
            {
                env.Options.AssertNoCatastrophicFailure();
            }
            catch
            {
                return true;
            }

            Thread.Sleep(20);
        }

        return false;
    }

    private static long ZeroOutTransactionRecord(string path, int fromTheEnd)
    {
        var journals = Directory.GetFiles(Path.Combine(path, "Journals"), "*.journal");
        Assert.Single(journals);

        var positions = new List<long>();
        using var file = new FileStream(journals[0], FileMode.Open, FileAccess.ReadWrite);

        var block = new byte[Constants.Storage.JournalPageSize];
        for (long offset = 0; offset + block.Length <= file.Length; offset += block.Length)
        {
            file.Position = offset;
            file.ReadExactly(block);

            if (BitConverter.ToUInt64(block, 0) == Constants.TransactionHeaderMarker)
                positions.Add(offset);
        }

        Assert.True(positions.Count > fromTheEnd + 1,
            $"expected more than {fromTheEnd + 1} transaction records in the journal, found {positions.Count}");

        var target = positions[^(fromTheEnd + 1)];
        file.Position = target;
        file.Write(new byte[block.Length]);
        file.Flush(flushToDisk: true);

        return target;
    }

    private static void InterlockedMax(ref int location, int value)
    {
        while (true)
        {
            var current = Volatile.Read(ref location);
            if (value <= current)
                return;
            if (Interlocked.CompareExchange(ref location, value, current) == current)
                return;
        }
    }
}
