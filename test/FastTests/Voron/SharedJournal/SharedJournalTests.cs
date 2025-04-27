using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Impl.Journal;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.SharedJournal;

public class SharedJournalTests(ITestOutputHelper output) : RavenTestBase(output)
{
    public class MyJournalMerger(ManualResetEventSlim e) : IJournalMerger
    {
        public bool IsIdle => true;

        public void JournalMergeSubmitted()
        {
            e.Set();
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanFlushRootEnvAfterJournalsFilledWithOnlyBranchCommits()
    {
        string rootPath = NewDataPath(suffix: "root");
        IOExtensions.DeleteDirectory(rootPath);

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 3 * 4096; // only two transactions per journal

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                string path = NewDataPath("branch");
                IOExtensions.DeleteDirectory(path);
                return CreateBranchEnv(path, root);
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            using var branchEnv = task.Result;
            Task secondBranchCommit = Task.CompletedTask;
            root.Journal.ForTestingPurposesOnly().OnWriteBuffersToJournal += q =>
            {
                int before = q.Count;
                secondBranchCommit = Task.Run(() =>
                {
                    using (var tx = branchEnv.WriteTransaction())
                    {
                        tx.CreateTree(Guid.NewGuid().ToString());
                        tx.Commit();
                    }
                });
                WaitForValue(() => q.Count, 1 + before);
            };

            using (var rootTx = root.WriteTransaction())
            {
                Tree tree = rootTx.CreateTree("rootTree");
                tree.Add("root", "yes");
                rootTx.Commit();
            }

            secondBranchCommit.Wait();

            root.FlushLogToDataFile();

            root.SyncDataFileImmediately();
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateRootAndBranchEnvironments()
    {
        string rootPath = NewDataPath(suffix: "root");
        IOExtensions.DeleteDirectory(rootPath);
        string branchPath = NewDataPath(suffix: "branch");
        IOExtensions.DeleteDirectory(branchPath);
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
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("root", "no");
                    tree.Add("branch", "yes");
                    branchTx.Commit();
                }
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
        }

        // here we restart the environments

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using var branch = CreateBranchEnv(branchPath, root);

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

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            // Now do another write
            var task = Task.Run(() =>
            {
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("try", "2");
                    branchTx.Commit();
                }
            }).ContinueWith(t =>
            {
                mre.Set();
                return t;
            }).Unwrap();

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
        }

        // here we restart the environments again

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
            branchOptions.ManualFlushing = true;
            branchOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();
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


    [RavenFact(RavenTestCategory.Voron)]
    public void WillRestoreMissingHardLinksOnRootRecovery()
    {
        string rootPath = NewDataPath(suffix: "root");
        IOExtensions.DeleteDirectory(rootPath);
        string branchPath = NewDataPath(suffix: "branch");
        IOExtensions.DeleteDirectory(branchPath);
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
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("root", "no");
                    tree.Add("branch", "yes");
                    branchTx.Commit();
                }
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
        }

        // here we restart the environments, but we'll pretend that we had a hard crash
        // and the links for the journal files for the branch were removed, so we'll need
        // to recover them during root recovery
        foreach (string journal in Directory.GetFiles(Path.Combine(branchPath, "Journals")))
        {
            File.Delete(journal);
        }

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using var branch = CreateBranchEnv(branchPath, root);

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

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            // Now do another write
            var task = Task.Run(() =>
            {
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("try", "2");
                    branchTx.Commit();
                }
            }).ContinueWith(t =>
            {
                mre.Set();
                return t;
            }).Unwrap();

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
        }

        // here we restart the environments again

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;

            using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
            branchOptions.ManualFlushing = true;
            branchOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();
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


    [RavenFact(RavenTestCategory.Voron)]
    public void CanFlushWithSharedJournals()
    {
        string rootPath = NewDataPath(suffix: "root");
        string branchPath = NewDataPath(suffix: "branch");
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
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);

            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("root", "no");
                    tree.Add("branch", "yes");
                    branchTx.Commit();
                }

                branch.FlushLogToDataFile();
                branch.SyncDataFileImmediately();
            });
            task.ContinueWith(_ => mre.Set());
            WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();
        }

        // here we restart the environments

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;


            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using (var rootTx = root.ReadTransaction())
            {
                Assert.Equal("yes", rootTx.ReadTree("rootTree").Read("root").Reader.ToString());
                Assert.Equal("no", rootTx.ReadTree("rootTree").Read("branch").Reader.ToString());
                Assert.Null(rootTx.ReadTree("branchTree"));
            }

            using var branch = CreateBranchEnv(branchPath, root);

            using (var branchTx = branch.ReadTransaction())
            {
                Assert.Null(branchTx.ReadTree("rootTree"));
                Assert.Equal("no", branchTx.ReadTree("branchTree").Read("root").Reader.ToString());
                Assert.Equal("yes", branchTx.ReadTree("branchTree").Read("branch").Reader.ToString());
            }

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            // Now do another write
            var task = Task.Run(() =>
            {
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("try", "2");
                    branchTx.Commit();
                }
            });
            task.ContinueWith(_ => mre.Set());
            WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            using (var rootTx = root.WriteTransaction())
            {
                Tree tree = rootTx.CreateTree("rootTree");
                tree.Add("again", "yes");
                rootTx.Commit();
            }

            root.FlushLogToDataFile();
            branch.FlushLogToDataFile();
            root.SyncDataFileImmediately();
            branch.SyncDataFileImmediately();
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void JournalsAreDeletesInRootAndBranch_MixedWrites()
    {
        string rootPath = NewDataPath(suffix: "-root");
        string branchPath = NewDataPath(suffix: "-branch");
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 4096 * 4;

            // journal 0 - 0
            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();
            // journal 0 - 1
            using (var rootTx = root.WriteTransaction())
            {
                Tree tree = rootTx.CreateTree("rootTree");
                tree.Add("root", "yes");
                tree.Add("branch", "no");
                rootTx.Commit();
            }

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                // journal 0 - 2
                // journal 0 - 3 - register link journals
                using var branch = CreateBranchEnv(branchPath, root);
                // journal 1 - 0
                using (var rootTx = root.WriteTransaction())
                {
                    Tree tree = rootTx.CreateTree("rootTree");
                    tree.Add("try", "one");
                    rootTx.Commit();
                }

                // journal 1 - 1
                using (var rootTx = root.WriteTransaction())
                {
                    Tree tree = rootTx.CreateTree("rootTree");
                    tree.Add("try", "two");
                    rootTx.Commit();
                }

                // journal 1 - 2
                // journal 1 - 3 - register link journals
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("root", "no");
                    tree.Add("branch", "yes");
                    branchTx.Commit();
                }

                int filesCountBefore = Directory.GetFiles(branch.Options.JournalPath.FullPath).Length;

                branch.FlushLogToDataFile();
                branch.SyncDataFileImmediately();

                int filesCountAfter = Directory.GetFiles(branch.Options.JournalPath.FullPath).Length;
                Assert.Equal(0, filesCountAfter);
                Assert.True(filesCountBefore > filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);


            Assert.Null(root.Journal.CurrentFile);

            int filesCountBefore = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();

            int filesCountAfter = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;
            Assert.Equal(0, filesCountAfter);

            Assert.True(filesCountBefore > filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void JournalsAreDeletedInRootAndBranch()
    {
        string rootPath = NewDataPath(suffix: "-root");
        string branchPath = NewDataPath(suffix: "-branch");
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 4096 * 3;

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
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("root", "no");
                    tree.Add("branch", "yes");
                    branchTx.Commit();
                }

                int filesCountBefore = Directory.GetFiles(branch.Options.JournalPath.FullPath).Length;

                branch.FlushLogToDataFile();
                branch.SyncDataFileImmediately();

                int filesCountAfter = Directory.GetFiles(branch.Options.JournalPath.FullPath).Length;

                Assert.True(filesCountBefore > filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
            int filesCountBefore = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();

            int filesCountAfter = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;

            Assert.True(filesCountBefore > filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
        }
    }

    private static StorageEnvironment CreateBranchEnv(string branchPath, StorageEnvironment root)
    {
        var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
        branchOptions.RootJournal = root.Journal;
        branchOptions.ManualFlushing = true;
        branchOptions.ManualSyncing = true;
        return new StorageEnvironment(branchOptions);
    }

    public static void WaitForTaskAndExecuteBranchTransactions(Task task, ManualResetEventSlim mre, StorageEnvironment root)
    {
        while (task.IsCompleted is false)
        {
            mre.Wait();
            mre.Reset();
            using (var tx = root.WriteTransaction())
            {
                tx.Commit();
            }
        }

        task.Wait();
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void CanProperlyHandleChangingDbId()
    {
        var path = NewDataPath();
        IOExtensions.DeleteDirectory(path);
        {
            using var opts = StorageEnvironmentOptions.ForPathForTests(path);
            opts.ManualSyncing = true;
            opts.ManualFlushing = true;
            // tx 1
            using (var env = new StorageEnvironment(opts))
            {
                env.FlushLogToDataFile();
                env.SyncDataFileImmediately();
                // tx 2
                using (var txw = env.WriteTransaction())
                {
                    txw.CreateTree("abc");
                    txw.Commit();
                }
            }
        }
        {
            using var opts = StorageEnvironmentOptions.ForPathForTests(path);
            opts.ManualSyncing = true;
            opts.ManualFlushing = true;
            opts.OwnsPagers = false;
            opts.GenerateNewDatabaseId = true;
            // tx 3 - changing db id
            using (var env = new StorageEnvironment(opts))
            {
                Assert.Equal(3, env.CurrentStateRecord.TransactionId);

                // tx 4 - with new id
                using (var txw = env.WriteTransaction())
                {
                    txw.CreateTree("def");
                    txw.Commit();
                }
            }
        }

        {
            using var opts = StorageEnvironmentOptions.ForPathForTests(path);
            opts.ManualSyncing = true;
            opts.ManualFlushing = true;
            opts.OwnsPagers = false;
            using (var env = new StorageEnvironment(opts))
            {
                // here we fail because we have a mix of tx in the journal, from multiple ids
            }
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void CanRecoverRootWhenLastJournalIsJustBranchCommits()
    {
        string rootPath = NewDataPath(suffix: "-root");
        string branchPath = NewDataPath(suffix: "-branch");
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 4096 * 3;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using (var rootTx = root.WriteTransaction())
            {
                Tree tree = rootTx.CreateTree("rootTree");
                tree.Add("root", "yes");
                tree.Add("branch", "no");
                rootTx.Commit();
            }

            root.FlushLogToDataFile();

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
                for (int i = 0; i < 10; i++)
                {
                    using (var branchTx = branch.WriteTransaction())
                    {
                        Tree tree = branchTx.CreateTree("branchTree");
                        tree.Add("root", i.ToString());
                        branchTx.Commit();
                    }

                }
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
        }

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 4096 * 4;
            using var root = new StorageEnvironment(rootOptions);

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();
        }
    }


    [RavenFact(RavenTestCategory.Voron)]
    public void CanRecoverRootWhenLastJournalIsJustBranchCommits_WithNoRootTransactionsAtAll()
    {
        string rootPath = NewDataPath(suffix: "-root");
        string branchPath = NewDataPath(suffix: "-branch");
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 4096 * 3;

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            for (int i = 0; i < 2; i++)
            {
                using (var rootTx = root.WriteTransaction())
                {
                    Tree tree = rootTx.CreateTree("rootTree");
                    tree.Add("root", i.ToString());
                    rootTx.Commit();
                }
            }

            // Previous 3 txs should cover all journal
            Assert.Null(root.Journal.CurrentFile);
            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();

            string[] rootJournals = Directory.GetFiles(root.Options.JournalPath.FullPath);
            Assert.Empty(rootJournals);

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new MyJournalMerger(mre);

            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
                for (int i = 0; i < 10; i++)
                {
                    using (var branchTx = branch.WriteTransaction())
                    {
                        Tree tree = branchTx.CreateTree("branchTree");
                        tree.Add("root", i.ToString());
                        branchTx.Commit();
                    }

                }
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
        }

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.MaxLogFileSize = 4096 * 3;
            using var root = new StorageEnvironment(rootOptions);

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task SharedJournalsCanHandleFailures()
    {
        using (var store = GetDocumentStore())
        {
            await new Users_ByName().ExecuteAsync(store);

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "John" });

                session.SaveChanges();
            }

            await Indexes.WaitForIndexingAsync(store);

            var database = await GetDatabase(store.Database);
            database.IndexStore.SharedJournals.Env.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = t => t.ThrowSimulateErrorOnCommitStage2();

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Bob" });

                session.SaveChanges();
            }

            await Assert.ThrowsAsync<InvalidOperationException>(() => Indexes.WaitForIndexingAsync(store));

            database.IndexStore.SharedJournals.Env.ForTestingPurposesOnly().ModifyNewLowLevelTransaction = null;

            await store.Maintenance.SendAsync(new DeleteIndexErrorsOperation());
            await store.Maintenance.SendAsync(new EnableIndexOperation(new Users_ByName().IndexName));

            using (var session = store.OpenSession())
            {
                session.Store(new User { Name = "Alice" });

                session.SaveChanges();
            }

            await Indexes.WaitForIndexingAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                var x = await session.Query<User, Users_ByName>().ToListAsync();
                Assert.Equal(3, x.Count);
            }
        }
    }

    [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Voron)]
    public async Task SharedJournalsEventsConfig()
    {
        using (var store = GetDocumentStore())
        {
            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            var options = database.IndexStore.SharedJournals.Env.Options;

            EventHandlerHasInvocations<StorageEnvironmentOptions>(options, ["OnDirectoryInitialize"]);
        }
    }

    public static void EventHandlerHasInvocations<T>(object target, HashSet<string> exclude = null)
    {
        Assert.True(typeof(T).IsAssignableFrom(target.GetType()));

        var events = typeof(T).GetEvents();
        foreach (var @event in events)
        {
            if (exclude?.Contains(@event.Name) == true)
                continue;

            var fieldInfo = typeof(T).GetField(@event.Name, BindingFlags.NonPublic | BindingFlags.Instance);
            var delegateValue = fieldInfo?.GetValue(target) as MulticastDelegate;
            Assert.True(delegateValue?.GetInvocationList().Length > 0, $"{@event.Name} is not wired");
        }
    }

    private class User
    {
        public string Name { get; set; }
    }

    private class Users_ByName : AbstractIndexCreationTask<User>
    {
        public Users_ByName()
        {
            Map = users => from u in users
                select new { u.Name };
        }
    }
}
