using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.SharedJournal;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Utils;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24069 : RavenTestBase
{
    public RavenDB_24069(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task SharedJournalsFallbackToUnsharedWhenHardLinkLimitIsReached()
    {
        using (var store = GetDocumentStore())
        {
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);

            // Indexes whose names end with _0 or _1 will throw HardLinkLimitExceededException
            // on their first LinkFiles call - simulating NTFS hitting the 1023 hard-link limit.
            // The catch in FlushMergedJournalEntries must clear RootJournal for those branches,
            // fire the callback, and fail the current commit. The index worker loop must then
            // catch the retriable exception and continue indexing in unshared mode.
            database.IndexStore.ForTestingPurposesOnly().BeforeIndexStart = index =>
            {
                if (index.Name.EndsWith("_0") == false && index.Name.EndsWith("_1") == false)
                    return;

                var branchOptions = index._environment.Options;
                branchOptions.ForTestingPurposesOnly().BeforeLinkFiles = _ =>
                {
                    // one-shot: clear so any later call (should not happen once RootJournal is null) doesn't throw
                    branchOptions.ForTestingPurposesOnly().BeforeLinkFiles = null;
                    throw new HardLinkLimitExceededException($"simulated hard-link limit for '{index.Name}'");
                };
            };

            for (int i = 0; i < 4; i++)
            {
                await store.Maintenance.SendAsync(new PutIndexesOperation(new IndexDefinition
                {
                    Name = $"Users/ByName_{i}",
                    Maps = { "from u in docs.Users select new { u.Name }" }
                }));
            }

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Joe" });
                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);

            var indexes = database.IndexStore.GetIndexes().ToList();
            Assert.Equal(4, indexes.Count);

            foreach (var idx in indexes)
            {
                bool expectedShared = idx.Name.EndsWith("_0") == false && idx.Name.EndsWith("_1") == false;
                bool actualShared = idx._environment.Options.RootJournal != null;
                Assert.True(expectedShared == actualShared, $"Index '{idx.Name}' expected shared={expectedShared} but was shared={actualShared}");
            }

            foreach (var idx in indexes)
            {
                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Advanced.AsyncRawQuery<User>($"from index '{idx.Name}' where Name = 'Joe'").ToListAsync();
                    Assert.Equal(1, results.Count);
                }
            }
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void BranchFallsBackToUnsharedAndRecoversCleanly()
    {
        // Verifies concern A from code review: after a branch env falls back from shared to
        // unshared mode mid-life, pre-fallback hard-linked journals in the branch directory
        // are NOT overwritten by subsequent unshared writes (they share an inode with the
        // root's journal - reuse would corrupt the root), and on restart the branch can
        // recover every committed transaction with monotonic journal numbers.

        string rootPath = NewDataPath(suffix: "root");
        IOExtensions.DeleteDirectory(rootPath);

        string branchPath = NewDataPath(suffix: "branch");
        IOExtensions.DeleteDirectory(branchPath);

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
                bo.ForTestingPurposesOnly().BeforeLinkFiles = _ =>
                {
                    bo.ForTestingPurposesOnly().BeforeLinkFiles = null;
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

    [RavenFact(RavenTestCategory.Voron)]
    public void BranchReSharesWithRootAfterRestartFollowingFallback()
    {
        string rootPath = NewDataPath(suffix: "root");
        IOExtensions.DeleteDirectory(rootPath);

        string branchPath = NewDataPath(suffix: "branch");
        IOExtensions.DeleteDirectory(branchPath);

        // ----- phase 1: drive a fallback (shared tx1, fail link, unshared tx2) -----
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 3 * 4096;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

            StorageEnvironmentOptions branchOptions = null;
            StorageEnvironment branch = null;
            try
            {
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

                // arm a one-shot hook so the next LinkFiles throws as if NTFS hit the limit
                var bo = branchOptions;
                bo.ForTestingPurposesOnly().BeforeLinkFiles = _ =>
                {
                    bo.ForTestingPurposesOnly().BeforeLinkFiles = null;
                    throw new HardLinkLimitExceededException("simulated hard-link limit");
                };

                // force the root to roll over so the next branch commit triggers a fresh LinkFiles call
                for (int i = 0; i < 4; i++)
                {
                    using var rootTx = root.WriteTransaction();
                    rootTx.CreateTree("rootTree").Add($"r{i}", "v");
                    rootTx.Commit();
                }

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
                    }

                    // retry after fallback - the first attempt's tx was rolled back when Commit threw,
                    // so re-adding "tx2" here is a fresh insert, not a duplicate-key write
                    using (var branchTx = branch.WriteTransaction())
                    {
                        branchTx.CreateTree("branchTree").Add("tx2", "value2");
                        branchTx.Commit();
                    }
                });
                tx2Task.ContinueWith(_ => mre.Set());
                SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(tx2Task, mre, root);

                Assert.Null(branchOptions.RootJournal);
            }
            finally
            {
                branch?.Dispose();
                branchOptions?.Dispose();
            }
        }

        // ----- phase 2: restart with no hook armed; the next branch commit must re-share -----
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

            using var branch = new StorageEnvironment(branchOptions);

            // Now shrink MaxLogFileSize so the next root commits roll over quickly. The setter is
            // read live from options on each commit, no need to recreate the env.
            rootOptions.MaxLogFileSize = 3 * 4096;

            for (int i = 0; i < 4; i++)
            {
                using var rootTx = root.WriteTransaction();
                rootTx.CreateTree("rootTree").Add($"r2_{i}", "v");
                rootTx.Commit();
            }

            var branchJournalsBefore = new HashSet<string>(Directory.GetFiles(Path.Combine(branchPath, "Journals"), "*.journal"));

            var tx3Task = Task.Run(() =>
            {
                using var branchTx = branch.WriteTransaction();
                branchTx.CreateTree("branchTree").Add("tx3", "value3");
                branchTx.Commit();
            });
            tx3Task.ContinueWith(_ => mre.Set());
            SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(tx3Task, mre, root);

            // branch must remain shared after the post-restart commit
            Assert.NotNull(branchOptions.RootJournal);

            // a new journal must have appeared in the branch dir, hard-linked to one of root's journal files
            // (the branch's local journal number is independent of root's, but a hard link is bit-identical
            // because the two paths point to the same inode/file content)
            var newBranchJournals = Directory.GetFiles(Path.Combine(branchPath, "Journals"), "*.journal")
                .Where(p => branchJournalsBefore.Contains(p) == false)
                .ToArray();

            Assert.NotEmpty(newBranchJournals);

            var rootJournalFiles = Directory.GetFiles(Path.Combine(rootPath, "Journals"), "*.journal");

            foreach (var branchFile in newBranchJournals)
            {
                var branchBytes = ReadAllBytesShared(branchFile);
                bool foundMatchingRoot = rootJournalFiles.Any(rootFile => branchBytes.SequenceEqual(ReadAllBytesShared(rootFile)));
                Assert.True(foundMatchingRoot,
                    $"new branch journal '{branchFile}' has no matching content in root - it was created unshared instead of hard-linked");
            }

            // sanity: every commit (shared and unshared) is still readable after restart
            using (var branchTx = branch.ReadTransaction())
            {
                var tree = branchTx.ReadTree("branchTree");
                Assert.Equal("value1", tree.Read("tx1").Reader.ToString());
                Assert.Equal("value2", tree.Read("tx2").Reader.ToString());
                Assert.Equal("value3", tree.Read("tx3").Reader.ToString());
            }
        }

        // ----- phase 3: second restart - recovery must walk linked-old, unshared-middle, linked-new -----
        // Branch dir layout at this point:
        //   N...:        hard links to root's pre-fallback journals      (shared content)
        //   ?:           branch's own unshared journal (the tx2 retry)   (branch-only content)
        //   ?:           hard link to root's post-restart journal (tx3)  (shared content)
        // This phase verifies that recovery transitions cleanly across BOTH boundaries
        // (linked->unshared and unshared->linked) and a fresh commit lands above all of them.
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

            using var branch = new StorageEnvironment(branchOptions);

            using (var branchTx = branch.ReadTransaction())
            {
                var tree = branchTx.ReadTree("branchTree");
                Assert.Equal("value1", tree.Read("tx1").Reader.ToString());
                Assert.Equal("value2", tree.Read("tx2").Reader.ToString());
                Assert.Equal("value3", tree.Read("tx3").Reader.ToString());
            }

            // a fresh commit after the second restart must still go through across the
            // persisted shared/unshared/shared journal layout
            var tx4Task = Task.Run(() =>
            {
                using var branchTx = branch.WriteTransaction();
                branchTx.CreateTree("branchTree").Add("tx4", "value4");
                branchTx.Commit();
            });
            tx4Task.ContinueWith(_ => mre.Set());
            SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(tx4Task, mre, root);

            var numbers = EnumerateJournalNumbers(branchPath);
            Assert.Equal(numbers.Length, numbers.Distinct().Count());

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

    private static byte[] ReadAllBytesShared(string path)
    {
        using var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        using var ms = new MemoryStream();
        fs.CopyTo(ms);
        return ms.ToArray();
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
