using System.Collections.Generic;
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
using Voron.Data.BTrees;
using Voron.Exceptions;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24069 : RavenTestBase
{
    public RavenDB_24069(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateRootAndManyBranchEnvironments_RavenDB_24069()
    {
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

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task SharedJournalsFallbackToUnsharedWhenHardLinkLimitIsReached_RavenDB_24069()
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
                branchOptions.ForTestingPurposes_BeforeLinkFiles = _ =>
                {
                    // one-shot: clear so any later call (should not happen once RootJournal is null) doesn't throw
                    branchOptions.ForTestingPurposes_BeforeLinkFiles = null;
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

}
