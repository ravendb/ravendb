using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.SharedJournal;

public class RavenDB_24345(ITestOutputHelper output) : RavenTestBase(output)
{
    // The root fsyncs the Journals folders of its branches during a data file sync. When none of them could be
    // opened (the only branch was deleted), the io_uring path returned without releasing its lock, so the next
    // such sync of the root blocked forever.
    [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Linux)]
    public void DataFileSyncMustNotHangAfterTheOnlyBranchWasDeleted()
    {
        string rootPath = NewDataPath();
        string deletedBranchPath = NewDataPath();

        using var rootOptions = CreateOptions(rootPath);
        using var root = new StorageEnvironment(rootOptions);
        Assert.SkipUnless(PalConfiguration.WriteMode == Pal.RvnWriteMode.IoRing, "directories are synced through io_uring only in IoRing write mode");
        using var _ = root.Journal.SharedJournalsScope();

        WriteToNewBranch(root, deletedBranchPath);
        IOExtensions.DeleteDirectory(deletedBranchPath);
        SyncDataFile(root, attempt: 1);

        // a branch folder is synced once per new link, so it takes another branch to reach the directory sync again
        WriteToNewBranch(root, NewDataPath());
        SyncDataFile(root, attempt: 2);
    }

    private static void WriteToNewBranch(StorageEnvironment root, string branchPath)
    {
        var mre = new ManualResetEventSlim(false);
        root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);
        var branchTask = Task.Run(() =>
        {
            using var branch = SharedJournalTests.CreateBranchEnv(branchPath, root);
            using var tx = branch.WriteTransaction();
            tx.CreateTree("items").Add("item", "value");
            tx.Commit();
        });
        branchTask.ContinueWith(_ => mre.Set());
        SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(branchTask, mre, root);
    }

    private static void SyncDataFile(StorageEnvironment root, int attempt)
    {
        RollJournals(root, count: 2);
        root.FlushLogToDataFile();

        using var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(root.Journal.Applicator);
        Assert.True(Task.Run(sync.SyncDataFile).Wait(TimeSpan.FromMinutes(1)), $"data file sync #{attempt} hung");
    }

    private static void RollJournals(StorageEnvironment env, int count)
    {
        long target = env.Journal.CurrentJournalIndex + count;
        var value = new byte[32 * 1024];
        while (env.Journal.CurrentJournalIndex < target)
        {
            Random.Shared.NextBytes(value);
            using var tx = env.WriteTransaction();
            tx.CreateTree("items").Add("item", new MemoryStream(value));
            tx.Commit();
        }
    }

    private static StorageEnvironmentOptions CreateOptions(string path)
    {
        StorageEnvironmentOptions options = StorageEnvironmentOptions.ForPathForTests(path);
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = 16 * Constants.Storage.PageSize;
        return options;
    }
}
