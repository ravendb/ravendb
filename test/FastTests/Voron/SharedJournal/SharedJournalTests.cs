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

            using var root = new StorageEnvironment(rootOptions);

            var mre = new ManualResetEventSlim(false);
            
            root.Journal.OnBranchJournalEntrySubmitted += () =>
            {
                mre.Set();
            };
            
            using (var rootTx = root.WriteTransaction())
            {
                Tree tree = rootTx.CreateTree("rootTree");
                tree.Add("root", "yes");
                tree.Add("branch", "no");
                rootTx.Commit();
            }

            var task = Task.Run(() =>
            {
                using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
                branchOptions.RootJournal = root.Journal;
                branchOptions.ManualFlushing = true;
                
                using var branch = new StorageEnvironment(branchOptions);
                using (var branchTx = branch.WriteTransaction())
                {
                    Tree tree = branchTx.CreateTree("branchTree");
                    tree.Add("root", "no");
                    tree.Add("branch", "yes");
                    branchTx.Commit();
                }
            });

            for (int i = 0; i < 2; i++)
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

        // here we restart the environments
        
        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
            using var branchOptions = StorageEnvironmentOptions.ForPathForTests(branchPath);
            using var root = new StorageEnvironment(rootOptions);
            branchOptions.RootJournal = root.Journal;
            using var branch = new StorageEnvironment(branchOptions);

            using (var rootTx = root.ReadTransaction())
            {
                Assert.Equal("yes", rootTx.ReadTree("rootTree").Read("root").Reader.ToString());
                Assert.Equal("no", rootTx.ReadTree("rootTree").Read("branch").Reader.ToString());
                
            }

            using (var branchTx = branch.ReadTransaction())
            {
                Assert.Equal("no", branchTx.ReadTree("rootTree").Read("root").Reader.ToString());
                Assert.Equal("yes", branchTx.ReadTree("rootTree").Read("branch").Reader.ToString());
            }
        }
    }
}
