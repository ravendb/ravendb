using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26653 : RavenTestBase
{
    public RavenDB_26653(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task GetAllStoragesEnvironment_must_include_SharedJournals_env()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Joe" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.NotNull(database.IndexStore.SharedJournals);

        var envs = database.GetAllStoragesEnvironment().ToList();

        var sharedJournals = envs.Where(x => x.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals).ToList();
        Assert.Single(sharedJournals);

        var entry = sharedJournals[0];
        Assert.Equal(Raven.Server.Config.Categories.IndexingConfiguration.SharedJournalsStorageName, entry.Name);
        Assert.Same(database.IndexStore.SharedJournals.Env, entry.Environment);
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task GetAllStoragesEnvironment_must_not_include_SharedJournals_env_when_disabled()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = r =>
                r.Settings[RavenConfiguration.GetKey(c => c.Indexing.DisableSharedJournals)] = "true"
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Joe" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.Null(database.IndexStore.SharedJournals);

        var envs = database.GetAllStoragesEnvironment().ToList();
        Assert.DoesNotContain(envs, x => x.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals);
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task Backup_type_list_must_not_include_SharedJournals_env()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User { Name = "Joe" });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);

        Assert.NotNull(database.IndexStore.SharedJournals);

        var backupTypes = new List<StorageEnvironmentWithType.StorageEnvironmentType>
        {
            StorageEnvironmentWithType.StorageEnvironmentType.Index,
            StorageEnvironmentWithType.StorageEnvironmentType.Documents,
            StorageEnvironmentWithType.StorageEnvironmentType.Configuration,
        };

        var envs = database.GetAllStoragesEnvironment(backupTypes).ToList();
        Assert.DoesNotContain(envs, x => x.Type == StorageEnvironmentWithType.StorageEnvironmentType.SharedJournals);
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task Branch_index_env_journals_must_be_marked_as_hard_links_and_excluded_from_aggregated_size()
    {
        using var store = GetDocumentStore(new Options
        {
            RunInMemory = false
        });

        await new Users_ByName().ExecuteAsync(store);

        using (var session = store.OpenAsyncSession())
        {
            for (int i = 0; i < 100; i++)
                await session.StoreAsync(new User { Name = "user-" + i });
            await session.SaveChangesAsync();
        }

        Indexes.WaitForIndexing(store);

        var database = await Databases.GetDocumentDatabaseInstanceFor(store);
        Assert.NotNull(database.IndexStore.SharedJournals);

        var rootEnv = database.IndexStore.SharedJournals.Env;
        Assert.All(rootEnv.Journal.Files, j => Assert.False(j.IsHardLinked,
            $"@SharedJournals root must not have hard-linked journals; journal #{j.Number} reported IsHardLinked=true"));

        var rootReport = rootEnv.GenerateSizeReport(includeTempBuffers: false);
        Assert.Equal(0, rootReport.HardLinkedJournalsInBytes);
        Assert.True(rootReport.JournalsInBytes > 0, "expected root env to have non-empty journals");

        var indexEnvs = database.GetAllStoragesEnvironment(new List<StorageEnvironmentWithType.StorageEnvironmentType>
        {
            StorageEnvironmentWithType.StorageEnvironmentType.Index
        }).ToList();
        Assert.NotEmpty(indexEnvs);

        long branchHardLinkedTotal = 0;
        long branchJournalsTotal = 0;
        long branchDataFiles = 0;
        foreach (var indexEnv in indexEnvs)
        {
            var branchEnv = indexEnv.Environment;

            Assert.All(branchEnv.Journal.Files, j => Assert.True(j.IsHardLinked,
                $"index '{indexEnv.Name}' branch env journal #{j.Number} must be hard-linked while shared journals enabled"));

            var branchReport = branchEnv.GenerateSizeReport(includeTempBuffers: false);
            Assert.Equal(branchReport.JournalsInBytes, branchReport.HardLinkedJournalsInBytes);
            branchJournalsTotal += branchReport.JournalsInBytes;
            branchHardLinkedTotal += branchReport.HardLinkedJournalsInBytes;
            branchDataFiles += branchReport.DataFilePhysicalSizeInBytes;
        }

        Assert.True(branchHardLinkedTotal > 0, "expected branch envs to have hard-linked journals");
        Assert.Equal(branchJournalsTotal, branchHardLinkedTotal);

        var totalsAfterDedup = database.GetSizeOnDisk();

        long expectedJournalsContribution = rootReport.JournalsInBytes;
        long docsAndConfigDataFiles = 0;
        long docsAndConfigJournals = 0;
        foreach (var env in database.GetAllStoragesEnvironment(new List<StorageEnvironmentWithType.StorageEnvironmentType>
                 {
                     StorageEnvironmentWithType.StorageEnvironmentType.Documents,
                     StorageEnvironmentWithType.StorageEnvironmentType.Configuration
                 }))
        {
            var r = env.Environment.GenerateSizeReport(includeTempBuffers: false);
            docsAndConfigDataFiles += r.DataFilePhysicalSizeInBytes;
            docsAndConfigJournals += r.JournalsInBytes - r.HardLinkedJournalsInBytes;
        }

        var expectedPhysical = rootReport.DataFilePhysicalSizeInBytes
                               + expectedJournalsContribution
                               + branchDataFiles
                               + docsAndConfigDataFiles
                               + docsAndConfigJournals;

        Assert.Equal(expectedPhysical, totalsAfterDedup.Physical.SizeInBytes);

        long naiveJournalsSum = rootReport.JournalsInBytes + branchJournalsTotal + docsAndConfigJournals;
        long dedupedJournalsSum = rootReport.JournalsInBytes + docsAndConfigJournals;
        Assert.True(dedupedJournalsSum < naiveJournalsSum,
            $"de-duped journals total ({dedupedJournalsSum}) should be smaller than the naive sum ({naiveJournalsSum})");
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Root_env_must_not_subtract_its_own_journal_bytes_after_reopen_with_stale_branch_link()
    {
        // After a root env that hosts branches is disposed and reopened, branch dirs still
        // hold hard links to its journal inodes - so the root's own journals show
        // IsHardLinked=true. GenerateSizeReport must not subtract those bytes on the root
        // side, or the root under-reports disk usage on every reopen.
        string rootPath = NewDataPath(suffix: "root");
        Raven.Server.Utils.IOExtensions.DeleteDirectory(rootPath);
        string branchPath = NewDataPath(suffix: "branch");
        Raven.Server.Utils.IOExtensions.DeleteDirectory(branchPath);

        // Phase 1: open root + one branch, commit through branch to materialize the hard link.
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _scope = root.Journal.SharedJournalsScope();

            var mre = new System.Threading.ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new FastTests.Voron.SharedJournal.SharedJournalTests.MyJournalMerger(mre);

            var task = System.Threading.Tasks.Task.Run(() =>
            {
                using var branch = FastTests.Voron.SharedJournal.SharedJournalTests.CreateBranchEnv(branchPath, root);
                using var btx = branch.WriteTransaction();
                btx.CreateTree("branchTree").Add("k", "v");
                btx.Commit();
            });
            task.ContinueWith(_ => mre.Set());
            FastTests.Voron.SharedJournal.SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            // ManualFlushing means the journal stays on disk - do NOT flush to data file here.
        }

        // Phase 2: branch dir still holds its hard link; reopen root alone and inspect.
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            // Re-enter shared-journals mode (server-side orchestrator does this via SharedIndexJournals).
            // Dedup reads BranchJournalMerger to know this env is the canonical owner.
            using var _scope = root.Journal.SharedJournalsScope();
            var mre = new System.Threading.ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new FastTests.Voron.SharedJournal.SharedJournalTests.MyJournalMerger(mre);

            Assert.NotEmpty(root.Journal.Files);

            // Branch dir still holds its link, so IsHardLinked honestly reports true.
            Assert.All(root.Journal.Files, j => Assert.True(j.IsHardLinked,
                $"Root's journal #{j.Number} should reflect OS-level nlinks > 1 (the branch link survives)"));

            // But these bytes belong to the root - dedup must not subtract.
            var sizeReport = root.GenerateSizeReport(includeTempBuffers: false);
            Assert.True(sizeReport.JournalsInBytes > 0, "root must report non-empty journals after reopen");
            Assert.Equal(0, sizeReport.HardLinkedJournalsInBytes);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void FallenBack_branch_must_still_subtract_its_old_hard_linked_journals()
    {
        // When a branch can no longer create hard links (e.g. OS hard-link limit reached)
        // it clears its RootJournal and switches to unshared mode. Its existing JournalFile
        // entries remain IsHardLinked=true (the root still holds those bytes); only new
        // local journals are IsHardLinked=false.
        //
        // The dedup math must still subtract those old hard-linked entries even though
        // the env's RootJournal is now null and the env no longer hosts branches.
        string rootPath = NewDataPath(suffix: "root");
        Raven.Server.Utils.IOExtensions.DeleteDirectory(rootPath);
        string branchPath = NewDataPath(suffix: "branch");
        Raven.Server.Utils.IOExtensions.DeleteDirectory(branchPath);

        using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
        rootOptions.ManualFlushing = true;
        rootOptions.ManualSyncing = true;

        using var root = new StorageEnvironment(rootOptions);
        using var _scope = root.Journal.SharedJournalsScope();

        var mre = new System.Threading.ManualResetEventSlim(false);
        root.Journal.BranchJournalMerger = new FastTests.Voron.SharedJournal.SharedJournalTests.MyJournalMerger(mre);

        StorageEnvironment branch = null;
        try
        {
            var task = System.Threading.Tasks.Task.Run(() =>
            {
                branch = FastTests.Voron.SharedJournal.SharedJournalTests.CreateBranchEnv(branchPath, root);
                using var btx = branch.WriteTransaction();
                btx.CreateTree("branchTree").Add("k", "v");
                btx.Commit();
            });
            task.ContinueWith(_ => mre.Set());
            FastTests.Voron.SharedJournal.SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            Assert.NotNull(branch);
            // Simulate the fallback: branch loses its RootJournal but the existing journal
            // entries (created via the branch-link path) keep IsHardLinked=true.
            branch.Options.RootJournal = null;

            Assert.NotEmpty(branch.Journal.Files);
            Assert.All(branch.Journal.Files, j => Assert.True(j.IsHardLinked,
                $"Pre-fallback branch journal #{j.Number} must remain IsHardLinked - root still owns the inode"));

            var sizeReport = branch.GenerateSizeReport(includeTempBuffers: false);
            Assert.True(sizeReport.JournalsInBytes > 0);
            // Root still owns these bytes; the fallen-back branch's size report must
            // subtract them so the cluster-wide sum doesn't double-count.
            Assert.Equal(sizeReport.JournalsInBytes, sizeReport.HardLinkedJournalsInBytes);
        }
        finally
        {
            branch?.Dispose();
        }
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users select new { u.Name };
        }
    }
}
