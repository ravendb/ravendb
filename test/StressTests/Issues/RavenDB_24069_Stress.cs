using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron.SharedJournal;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Exceptions;
using Xunit;

namespace StressTests.Issues;

public class RavenDB_24069_Stress : RavenTestBase
{
    public RavenDB_24069_Stress(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateRootAndManyBranchEnvironments()
    {
        // Stress test - spins up 1024 on-disk branch StorageEnvironments against a single
        // root, hitting NTFS's 1023 hard-link-per-inode limit and exercising the per-branch
        // fallback to unshared journal mode. Lives in StressTests because the I/O cost
        // (~1024 journal directories, restart cycle x3) makes it unsuitable for SlowTests.
        // The functional fallback behaviour is also covered by deterministic, hook-based
        // tests in SlowTests.Issues.RavenDB_24069.

        string rootPath = NewDataPath(suffix: "root");
        IOExtensions.DeleteDirectory(rootPath);
        var branches = new List<string>();
        var tasks = new List<Task>();

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using (var rootTx = root.WriteTransaction())
            {
                Tree tree = rootTx.CreateTree("rootTree");
                tree.Add("root", "yes");
                tree.Add("branch", "no");
                rootTx.Commit();
            }

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

            for (int i = 0; i < 1024; i++)
            {
                string branchPath = NewDataPath(suffix: "branch");
                IOExtensions.DeleteDirectory(branchPath);
                branches.Add(branchPath);

                var task = Task.Run(() =>
                {
                    using var branch = SharedJournalTests.CreateBranchEnv(branchPath, root);
                    while (true)
                    {
                        try
                        {
                            using (var branchTx = branch.WriteTransaction())
                            {
                                Tree tree = branchTx.CreateTree("branchTree");
                                tree.Add("root", "no");
                                tree.Add("branch", "yes");
                                branchTx.Commit();
                            }
                            break;
                        }
                        catch (HardLinkLimitExceededException)
                        {
                            // Hard-link limit reached: RootJournal was cleared, retry in unshared mode
                        }
                    }
                });
                task.ContinueWith(_ => mre.Set());
                tasks.Add(task);
            }

            foreach (var task in tasks)
            {
                SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);
            }
        }
        tasks.Clear();
        // here we restart the environments

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();
            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

            foreach (var branchPath in branches)
            {
                // Now do another write
                var task = Task.Run(() =>
                {
                    using var branch = SharedJournalTests.CreateBranchEnv(branchPath, root);
                    using (var rootTx = root.ReadTransaction())
                    {
                        Assert.Equal("yes", rootTx.ReadTree("rootTree").Read("root").Reader.ToString());
                        Assert.Equal("no", rootTx.ReadTree("rootTree").Read("branch").Reader.ToString());
                        Assert.Null(rootTx.ReadTree("branchTree"));
                    }

                    using (var branchTx = branch.ReadTransaction())
                    {
                        Assert.Null(branchTx.ReadTree("rootTree"));
                        Assert.Equal("no", branchTx.ReadTree("branchTree").Read("root").Reader.ToString());
                        Assert.Equal("yes", branchTx.ReadTree("branchTree").Read("branch").Reader.ToString());
                    }

                    while (true)
                    {
                        try
                        {
                            using (var branchTx = branch.WriteTransaction())
                            {
                                Tree tree = branchTx.CreateTree("branchTree");
                                tree.Add("try", "2");
                                branchTx.Commit();
                            }
                            break;
                        }
                        catch (HardLinkLimitExceededException)
                        {
                            // Hard-link limit reached: RootJournal was cleared, retry in unshared mode
                        }
                    }
                }).ContinueWith(t =>
                {
                    mre.Set();
                    return t;
                }).Unwrap();

                tasks.Add(task);
            }

            foreach (var task in tasks)
            {
                SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);
            }
        }
        tasks.Clear();
        // here we restart the environments again

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            foreach (var branchPath in branches)
            {
                using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
                branchOptions.ManualFlushing = true;
                branchOptions.ManualSyncing = true;

                branchOptions.RootJournal = root.Journal;
                using var branch = new StorageEnvironment(branchOptions);

                using (var rootTx = root.ReadTransaction())
                {
                    Assert.Equal("yes", rootTx.ReadTree("rootTree").Read("root").Reader.ToString());
                    Assert.Equal("no", rootTx.ReadTree("rootTree").Read("branch").Reader.ToString());
                    Assert.Null(rootTx.ReadTree("branchTree"));
                }

                using (var branchTx = branch.ReadTransaction())
                {
                    Assert.Null(branchTx.ReadTree("rootTree"));
                    Assert.Equal("no", branchTx.ReadTree("branchTree").Read("root").Reader.ToString());
                    Assert.Equal("yes", branchTx.ReadTree("branchTree").Read("branch").Reader.ToString());
                    Assert.Equal("2", branchTx.ReadTree("branchTree").Read("try").Reader.ToString());
                }
            }
        }
    }
}
