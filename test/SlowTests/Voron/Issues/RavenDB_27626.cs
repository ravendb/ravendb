using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using FastTests;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27626(ITestOutputHelper output) : RavenTestBase(output)
{
    private const long JournalSize = 256 * 1024;

    // The pool preparation thread creates the recyclable journal at its full size and may hold the zeros back for a while.
    // An environment opened on the same directory in the meantime must not adopt that file as its next journal, or the
    // pending zeros land on top of the transactions it writes there.
    [RavenFact(RavenTestCategory.Voron)]
    public void JournalStillBeingZeroedByThePreviousInstanceIsNotReusedByTheNextOne()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);
        var journalsPath = Path.Combine(path, "Journals");

        using var enteredZeroing = new ManualResetEventSlim();
        using var releaseZeroing = new ManualResetEventSlim();
        using var zeroingDone = new ManualResetEventSlim();

        try
        {
            using (var options = CreateOptions(path, recoveryErrors: []))
            {
                options.EnableJournalPoolPrewarming = true;
                options.ForTestingPurposesOnly().OnJournalZeroingPacing = () =>
                {
                    enteredZeroing.Set();
                    releaseZeroing.Wait();
                };
                options.ForTestingPurposesOnly().AfterJournalZeroing = zeroingDone.Set;

                using var env = new StorageEnvironment(options);
                env.WriteFlow.ForTestingPurposesOnly().ForceZeroedJournalPreparation = true;

                for (int i = 0; enteredZeroing.Wait(0) == false; i++)
                {
                    Assert.True(i < 1000, "the half-fill trigger did not start preparing a pool journal");
                    Write(env, "before" + i);
                }
            } // disposed while the zeroing thread is parked between creating the file and writing the zeros

            var recoveryErrors = new List<string>();
            int written = 0;
            using (var options = CreateOptions(path, recoveryErrors))
            using (var env = new StorageEnvironment(options))
            {
                // roll to a new journal, which is where a gathered pool file would be taken from
                var journalsBefore = Directory.GetFiles(journalsPath, "*.journal").Length;
                while (Directory.GetFiles(journalsPath, "*.journal").Length == journalsBefore)
                {
                    Assert.True(written < 1000, "no journal roll happened");
                    Write(env, "after" + written++);
                }

                Write(env, "after" + written++);
            }

            releaseZeroing.Set();
            Assert.True(zeroingDone.Wait(TimeSpan.FromMinutes(1)), "the pool preparation did not finish");

            using (var options = CreateOptions(path, recoveryErrors))
            using (var env = new StorageEnvironment(options))
            {
                Assert.Empty(recoveryErrors);

                using var tx = env.ReadTransaction();
                var tree = tx.ReadTree("t");
                for (int i = 0; i < written; i++)
                    Assert.True(tree.Read("after" + i) != null, $"after{i} must survive the recovery");
            }
        }
        finally
        {
            releaseZeroing.Set(); // the zeroing thread is shared by every environment in the process
        }
    }

    private static void Write(StorageEnvironment env, string key)
    {
        var value = new byte[24 * 1024];
        Random.Shared.NextBytes(value); // incompressible, so the transactions fill the small journals
        using var tx = env.WriteTransaction();
        tx.CreateTree("t").Add(key, new MemoryStream(value));
        tx.Commit();
    }

    private static StorageEnvironmentOptions CreateOptions(string path, List<string> recoveryErrors)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(path);
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = JournalSize;
        options.InitialLogFileSize = JournalSize;
        options.EnableJournalPoolPrewarming = false;
        options.OnRecoveryError += (_, e) => recoveryErrors.Add(e.Message);
        return options;
    }
}
