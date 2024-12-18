using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Voron.SharedJournal;

public class SharedJournalTests(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CanCreateRootAndBranchEnvironments()
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
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };
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

            using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
            branchOptions.ManualFlushing = true;
            branchOptions.ManualSyncing = true;

            using var root = new StorageEnvironment(rootOptions);
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
            }

            var mre = new ManualResetEventSlim(false);
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };
            // Now do another write
            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
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
            } ).Unwrap();

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
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };
            
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
            }
            
            var mre = new ManualResetEventSlim(false);
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };
            // Now do another write
            var task = Task.Run(() =>
            {
                using var branch = CreateBranchEnv(branchPath, root);
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
            rootOptions.MaxLogFileSize = 4096 * 3;

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
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };

            var task = Task.Run(() =>
            {
                // journal 0 - 2
                using var branch = CreateBranchEnv(branchPath, root);
                // journal 1 - 0
                using (var rootTx = root.WriteTransaction())
                {
                    Tree tree = rootTx.CreateTree("rootTree");
                    tree.Add("try", "one");
                    rootTx.Commit();
                }
                
                // journal 1 - 1
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("root", "no");
                    tree.Add("branch", "yes");
                    branchTx.Commit();
                }
                
                // journal 1 - 2
                using (var rootTx = root.WriteTransaction())
                {
                    Tree tree = rootTx.CreateTree("rootTree");
                    tree.Add("try", "two");
                    rootTx.Commit();
                }

                int filesCountBefore = Directory.GetFiles(branch.Options.JournalPath.FullPath).Length;

                branch.FlushLogToDataFile();
                branch.SyncDataFileImmediately();
                
                int filesCountAfter = Directory.GetFiles(branch.Options.JournalPath.FullPath).Length;
                Assert.Equal(0, filesCountAfter);
                Assert.True(filesCountBefore >  filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            
            Assert.Null(root.Journal.CurrentFile);
            
            int filesCountBefore = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();
            
            int filesCountAfter = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;
            Assert.Equal(0, filesCountAfter);

            Assert.True(filesCountBefore >  filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
        }
    }

    
    [RavenFact(RavenTestCategory.Voron)]
    public void JournalsAreDeletesInRootAndBranch()
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
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };

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
                
                Assert.True(filesCountBefore >  filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
            });
            task.ContinueWith(_ => mre.Set());

            WaitForTaskAndExecuteBranchTransactions(task, mre, root);
            int filesCountBefore = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();
            
            int filesCountAfter = Directory.GetFiles(root.Options.JournalPath.FullPath).Length;
                
            Assert.True(filesCountBefore >  filesCountAfter, $"{filesCountBefore} > {+filesCountAfter}");
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
        while(task.IsCompleted is false)
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
}
