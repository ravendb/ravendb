using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Voron;
using FastTests.Voron.SharedJournal;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24069_Voron : StorageTest
{
    public RavenDB_24069_Voron(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void BranchFallsBackToUnsharedAndRecoversCleanly()
    {
        // Verifies concern A from code review: after a branch env falls back from shared to
        // unshared mode mid-life, pre-fallback hard-linked journals in the branch directory
        // are NOT overwritten by subsequent unshared writes (they share an inode with the
        // root's journal - reuse would corrupt the root), and on restart the branch can
        // recover every committed transaction with monotonic journal numbers.

        string rootPath = Path.Combine(DataDir, "root");
        string branchPath = Path.Combine(DataDir, "branch");

        // ----- phase 1: run shared, force a successful link, then simulate hard-link limit -----
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 3 * 4096; // forces rollover after ~two transactions per journal

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

            StorageEnvironmentOptions branchOptions = null;
            StorageEnvironment branch = null;
            try
            {
                // tx1: create the branch env and do one commit; lands on the root's current journal and
                // gets linked into the branch's dir. The commit goes through SubmitBranchJournalEntry,
                // so it must run on a worker while the main thread drives the root's merger.
                var createTask = Task.Run(() =>
                {
                    branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
                    branchOptions.ManualFlushing = true;
                    branchOptions.ManualSyncing = true;
                    branchOptions.RootJournal = root.Journal;
                    branch = new StorageEnvironment(branchOptions);

                    using var branchTx = branch.WriteTransaction();
                    branchTx.CreateTree("branchTree").Add("tx1", "value1");
                    branchTx.Commit();
                });
                createTask.ContinueWith(_ => mre.Set());
                SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(createTask, mre, root);

                // sanity: branch dir has at least one linked journal file
                var linkedJournals = Directory.GetFiles(Path.Combine(branchPath, "Journals"), "*.journal").ToArray();
                Assert.NotEmpty(linkedJournals);
                var preFallbackJournalSizes = linkedJournals.ToDictionary(p => p, p => new FileInfo(p).Length);

                // arm a one-shot hook: the next LinkFiles throws as if NTFS hit the hard-link limit
                var bo = branchOptions;
                bo.ForTestingPurposes_BeforeLinkFiles = _ =>
                {
                    bo.ForTestingPurposes_BeforeLinkFiles = null;
                    throw new HardLinkLimitExceededException("simulated hard-link limit");
                };

                // force the root to roll over so the next branch commit triggers a fresh LinkFiles call
                for (int i = 0; i < 4; i++)
                {
                    using var rootTx = root.WriteTransaction();
                    rootTx.CreateTree("rootTree").Add($"r{i}", "v");
                    rootTx.Commit();
                }

                // tx2: attempt 1 hits the armed hook -> LinkFiles throws -> branch transitions to
                // unshared mode (RootJournal cleared). Attempt 2 commits to the branch's own journal.
                bool fellBack = false;
                var tx2Task = Task.Run(() =>
                {
                    try
                    {
                        using var branchTx = branch.WriteTransaction();
                        branchTx.CreateTree("branchTree").Add("tx2", "value2");
                        branchTx.Commit();
                    }
                    catch (HardLinkLimitExceededException)
                    {
                        fellBack = true;
                    }

                    using (var branchTx = branch.WriteTransaction())
                    {
                        branchTx.CreateTree("branchTree").Add("tx2", "value2");
                        branchTx.Commit();
                    }
                });
                tx2Task.ContinueWith(_ => mre.Set());
                SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(tx2Task, mre, root);

                Assert.True(fellBack, "expected the hook to drive the branch into unshared mode");
                Assert.Null(branchOptions.RootJournal);

                // tx3: pure unshared write, no interaction with root's merger required
                using (var branchTx = branch.WriteTransaction())
                {
                    branchTx.CreateTree("branchTree").Add("tx3", "value3");
                    branchTx.Commit();
                }

                // pre-fallback linked journals must NOT have been touched by the unshared writes
                // (same inode as the root - any mutation would corrupt the root's journal)
                foreach (var (path, size) in preFallbackJournalSizes)
                {
                    Assert.True(File.Exists(path), $"pre-fallback linked journal '{path}' was deleted");
                    Assert.Equal(size, new FileInfo(path).Length);
                }

                // journal numbers in the branch dir must be unique (EnumerateJournalNumbers returns them sorted)
                var numbers = EnumerateJournalNumbers(branchPath);
                Assert.Equal(numbers.Length, numbers.Distinct().Count());
            }
            finally
            {
                branch?.Dispose();
                branchOptions?.Dispose();
            }
        }

        // ----- phase 2: reopen the branch in shared mode; recovery must see all three txs -----
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

            using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
            branchOptions.ManualFlushing = true;
            branchOptions.ManualSyncing = true;
            branchOptions.RootJournal = root.Journal;

            using (var branch = new StorageEnvironment(branchOptions))
            {
                using (var branchTx = branch.ReadTransaction())
                {
                    var tree = branchTx.ReadTree("branchTree");
                    Assert.Equal("value1", tree.Read("tx1").Reader.ToString());
                    Assert.Equal("value2", tree.Read("tx2").Reader.ToString());
                    Assert.Equal("value3", tree.Read("tx3").Reader.ToString());
                }

                var numbersBefore = EnumerateJournalNumbers(branchPath);
                var maxBefore = numbersBefore.Max();

                // one more commit after recovery must land above every existing journal number
                var tx4Task = Task.Run(() =>
                {
                    using var branchTx = branch.WriteTransaction();
                    branchTx.CreateTree("branchTree").Add("tx4", "value4");
                    branchTx.Commit();
                });
                tx4Task.ContinueWith(_ => mre.Set());
                SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(tx4Task, mre, root);

                var numbersAfter = EnumerateJournalNumbers(branchPath);
                Assert.True(numbersAfter.Max() > maxBefore,
                    $"new commit did not produce a higher journal number (before max={maxBefore}, after=[{string.Join(",", numbersAfter)}])");
                Assert.Equal(numbersAfter.Length, numbersAfter.Distinct().Count());

                using (var branchTx = branch.ReadTransaction())
                {
                    var tree = branchTx.ReadTree("branchTree");
                    Assert.Equal("value1", tree.Read("tx1").Reader.ToString());
                    Assert.Equal("value2", tree.Read("tx2").Reader.ToString());
                    Assert.Equal("value3", tree.Read("tx3").Reader.ToString());
                    Assert.Equal("value4", tree.Read("tx4").Reader.ToString());
                }
            }
        }
    }

    private static long[] EnumerateJournalNumbers(string basePath)
    {
        return Directory.GetFiles(Path.Combine(basePath, "Journals"), "*.journal")
            .Select(p => Path.GetFileNameWithoutExtension(p))
            .Select(name => long.TryParse(name, NumberStyles.Integer, CultureInfo.InvariantCulture, out var n) ? n : -1)
            .Where(n => n >= 0)
            .OrderBy(n => n)
            .ToArray();
    }
}
