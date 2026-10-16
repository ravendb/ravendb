using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.SharedJournal;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_27667 : RavenTestBase
{
    public RavenDB_27667(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Branch_whose_retired_journal_cannot_be_donated_deletes_it_and_keeps_working()
    {
        string rootPath = NewDataPath(suffix: "-root");
        string branchPath = NewDataPath(suffix: "-branch");
        IOExtensions.DeleteDirectory(rootPath);
        IOExtensions.DeleteDirectory(branchPath);

        using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
        rootOptions.ManualFlushing = true;
        rootOptions.ManualSyncing = true;
        rootOptions.MaxLogFileSize = 4096 * 8;
        rootOptions.EnableJournalPoolPrewarming = false; // the test asserts exact pool contents

        using var root = new StorageEnvironment(rootOptions);
        using var scope = root.Journal.SharedJournalsScope();

        var mre = new ManualResetEventSlim(false);
        root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

        var openBranch = Task.Run(() => SharedJournalTests.CreateBranchEnv(branchPath, root));
        openBranch.ContinueWith(_ => mre.Set());
        SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(openBranch, mre, root);

        using var branch = openBranch.Result;

        string rootJournals = root.Options.JournalPath.FullPath;
        string branchJournals = branch.Options.JournalPath.FullPath;

        WriteThroughRoot(branch, root, mre, from: 0, count: 40);
        Assert.True(Directory.GetFiles(branchJournals, "*.journal").Length > 1, "expected the writes to span several shared journals");

        ReleaseRootLinks(root);

        Assert.Empty(GetRecyclable(rootJournals));

        branch.FlushLogToDataFile();

        int probes = 0;
        branch.Options.ForTestingPurposesOnly().BeforeJournalLinkProbe = () =>
        {
            Interlocked.Increment(ref probes);
            throw new IOException("No space left on device");
        };

        int journalsBefore = Directory.GetFiles(branchJournals, "*.journal").Length;

        branch.SyncDataFileImmediately();

        Assert.False(branch.Options.IsCatastrophicFailureSet, "a journal that cannot be donated to the pool must not fail the environment");
        Assert.True(probes > 0, "the sync should have tried to donate the retired journals");
        Assert.True(Directory.GetFiles(branchJournals, "*.journal").Length < journalsBefore, "the journals that could not be donated should have been deleted");
        Assert.Empty(GetRecyclable(rootJournals));

        branch.Options.ForTestingPurposesOnly().BeforeJournalLinkProbe = null;

        WriteThroughRoot(branch, root, mre, from: 40, count: 40);
        ReleaseRootLinks(root);
        int recyclableBeforeBranchSync = GetRecyclable(rootJournals).Length;

        branch.FlushLogToDataFile();
        branch.SyncDataFileImmediately();

        Assert.True(GetRecyclable(rootJournals).Length > recyclableBeforeBranchSync, "with the probe working again, the branch should donate its retired journals to the root's pool");

        using (var tx = branch.ReadTransaction())
        {
            var tree = tx.ReadTree("branchTree");
            Assert.NotNull(tree.Read("item/0"));
            Assert.NotNull(tree.Read("item/79"));
        }
    }

    private static void WriteThroughRoot(StorageEnvironment branch, StorageEnvironment root, ManualResetEventSlim mre, int from, int count)
    {
        var writes = Task.Run(() =>
        {
            var bytes = new byte[2048];
            new Random(42).NextBytes(bytes);

            for (int i = from; i < from + count; i++)
            {
                using var tx = branch.WriteTransaction();
                tx.CreateTree("branchTree").Add($"item/{i}", new MemoryStream(bytes));
                tx.Commit();
            }
        });
        writes.ContinueWith(_ => mre.Set());
        SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(writes, mre, root);
    }

    private static void ReleaseRootLinks(StorageEnvironment root)
    {
        // an empty commit does not move the root's flush past the shared journals, so its sync would keep their links
        using (var tx = root.WriteTransaction())
        {
            tx.CreateTree("rootTree").Add($"marker/{Guid.NewGuid()}", new MemoryStream(new byte[16]));
            tx.Commit();
        }

        root.FlushLogToDataFile();
        root.SyncDataFileImmediately();
    }

    private static string[] GetRecyclable(string journalPath) =>
        Directory.GetFiles(journalPath, $"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}.*");
}
