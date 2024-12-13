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
            }).ContinueWith(_ => mre.Set());

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
            }).ContinueWith(_ => mre.Set());

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
