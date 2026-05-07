using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.SharedJournal;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26559 : RavenTestBase
{
    public RavenDB_26559(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void BranchOpenSucceeds_WhenLinkedJournalIsSmallerThanBranchInitialLogFileSize()
    {
        // Reproduces a production hazard with shared journals:
        //   - Root is configured with MaxLogFileSize smaller than the default 64KB
        //     InitialLogFileSize (the setter also caps Initial down). On-disk journals
        //     end up smaller than 64KB.
        //   - On reopen, branch options use the 64KB InitialLogFileSize default.
        //     EnsureMinimumSize during branch recovery sees journalLength < 64KB and tries
        //     to extend the file via FileInfo.Open(FileMode.OpenOrCreate), which uses the
        //     restrictive FileShare.None.
        //   - Root has the same hard-linked file open through its Pager, so on Windows
        //     this fails with IOException ("file is being used by another process").
        // A branch should not extend journals owned by root - branches in shared mode
        // should either skip EnsureMinimumSize or use share-friendly flags.

        string rootPath = NewDataPath(suffix: "root");
        IOExtensions.DeleteDirectory(rootPath);
        string branchPath = NewDataPath(suffix: "branch");
        IOExtensions.DeleteDirectory(branchPath);

        // ----- setup: create a 12KB linked journal -----
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 3 * 4096; // 12KB; the setter caps InitialLogFileSize down

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

            StorageEnvironmentOptions branchOptions = null;
            StorageEnvironment branch = null;
            try
            {
                var setupTask = Task.Run(() =>
                {
                    branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
                    branchOptions.ManualFlushing = true;
                    branchOptions.ManualSyncing = true;
                    branchOptions.MaxLogFileSize = 3 * 4096;
                    branchOptions.RootJournal = root.Journal;
                    branch = new StorageEnvironment(branchOptions);

                    using var branchTx = branch.WriteTransaction();
                    branchTx.CreateTree("branchTree").Add("k", "v");
                    branchTx.Commit();
                });
                setupTask.ContinueWith(_ => mre.Set());
                SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(setupTask, mre, root);
            }
            finally
            {
                branch?.Dispose();
                branchOptions?.Dispose();
            }
        }

        // ----- trigger: root keeps the 12KB cap so it doesn't pre-extend; branch open
        //                uses the default 64KB InitialLogFileSize and tries to extend
        //                the file root currently holds -----
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 3 * 4096; // root's recovery skips EnsureMinimumSize

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
            branchOptions.ManualFlushing = true;
            branchOptions.ManualSyncing = true;
            // Deliberately do NOT mirror root's MaxLogFileSize - branch's InitialLogFileSize
            // stays at the 64KB default while the on-disk journal is 12KB.
            branchOptions.RootJournal = root.Journal;

            // Branch ctor should succeed. Currently fails with InvalidOperationException
            // wrapping IOException ("file is being used by another process").
            using var branch = new StorageEnvironment(branchOptions);

            using (var branchTx = branch.ReadTransaction())
            {
                Assert.Equal("v", branchTx.ReadTree("branchTree").Read("k").Reader.ToString());
            }
        }
    }
}
