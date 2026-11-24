using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Xunit;

namespace FastTests.Voron.SharedJournal;

// An unreadable header record makes every entry of a shared journal fail validation. Each environment that has
// a link to the file must report it instead of treating the file as empty. A branch with no link is not affected.
public class RavenDB_27673(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int TransactionsAfterSync = 6;

    [RavenFact(RavenTestCategory.Voron)]
    public void UnreadableHeaderRecordOfTheNewestSharedJournalIsReportedByEveryEnvironmentWithALinkToIt()
    {
        string rootPath = NewDataPath(suffix: "-root");
        string branchAPath = NewDataPath(suffix: "-branchA");
        string branchBPath = NewDataPath(suffix: "-branchB");
        IOExtensions.DeleteDirectory(rootPath);
        IOExtensions.DeleteDirectory(branchAPath);
        IOExtensions.DeleteDirectory(branchBPath);

        {
            using var rootOptions = CreateOptions(rootPath, recoveryErrors: []);
            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using (var tx = root.WriteTransaction())
            {
                tx.CreateTree("rootTree").Add("root", "yes");
                tx.Commit();
            }

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

            // synced prefix of both branches
            var task = Task.Run(() =>
            {
                var branchA = OpenBranch(branchAPath, root, recoveryErrors: []);
                var branchB = OpenBranch(branchBPath, root, recoveryErrors: []);

                Write(branchA, "treeA", "a0");
                Write(branchB, "treeB", "b0");

                foreach (var branch in new[] { branchA, branchB })
                {
                    branch.FlushLogToDataFile();
                    branch.SyncDataFileImmediately();
                }

                return (branchA, branchB);
            });
            task.ContinueWith(_ => mre.Set());
            SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);
            var (branchA, branchB) = task.Result;

            root.FlushLogToDataFile();
            root.SyncDataFileImmediately();

            // unsynced suffix of branch A only, rolling over several small journals
            mre.Reset();
            var suffix = Task.Run(() =>
            {
                for (int i = 1; i <= TransactionsAfterSync; i++)
                    Write(branchA, "treeA", "a" + i, valueSize: 8 * 1024);
            });
            suffix.ContinueWith(_ => mre.Set());
            SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(suffix, mre, root);

            branchB.Dispose();
            branchA.Dispose();
        }

        var newestJournal = Directory.GetFiles(Path.Combine(rootPath, "Journals"), "*.journal").Max();
        Assert.Contains(Directory.GetFiles(Path.Combine(branchAPath, "Journals"), "*.journal"), f => IsSameFile(f, newestJournal));
        Assert.DoesNotContain(Directory.GetFiles(Path.Combine(branchBPath, "Journals"), "*.journal"), f => IsSameFile(f, newestJournal));

        using (var fs = new FileStream(newestJournal, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            fs.Write(new byte[4 * 1024]);

        var rootErrors = new List<string>();
        using var reopenedRootOptions = CreateOptions(rootPath, rootErrors);
        using var reopenedRoot = new StorageEnvironment(reopenedRootOptions);
        using var __ = reopenedRoot.Journal.SharedJournalsScope();

        Assert.Contains(rootErrors, e => e.Contains("unreadable header record"));
        Assert.DoesNotContain(rootErrors, e => e.Contains("Unrecoverable database"));
        using (var tx = reopenedRoot.ReadTransaction())
            Assert.Equal("yes", tx.ReadTree("rootTree").Read("root").Reader.ToString());

        var branchBErrors = new List<string>();
        using (var branchB = OpenBranch(branchBPath, reopenedRoot, branchBErrors))
        using (var tx = branchB.ReadTransaction())
        {
            Assert.Empty(branchBErrors);
            Assert.NotNull(tx.ReadTree("treeB").Read("b0"));
        }

        var branchAErrors = new List<string>();
        using (var branchA = OpenBranch(branchAPath, reopenedRoot, branchAErrors))
        using (var tx = branchA.ReadTransaction())
        {
            Assert.Contains(branchAErrors, e => e.Contains("unreadable header record"));
            var tree = tx.ReadTree("treeA");
            Assert.NotNull(tree.Read("a0"));
            Assert.Null(tree.Read("a" + TransactionsAfterSync));
        }
    }

    private static void Write(StorageEnvironment env, string tree, string key, int valueSize = 16)
    {
        var value = new byte[valueSize];
        Random.Shared.NextBytes(value);
        using var tx = env.WriteTransaction();
        tx.CreateTree(tree).Add(key, new MemoryStream(value));
        tx.Commit();
    }

    private static bool IsSameFile(string a, string b) => File.ReadAllBytes(a).AsSpan().SequenceEqual(File.ReadAllBytes(b));

    private static StorageEnvironment OpenBranch(string path, StorageEnvironment root, List<string> recoveryErrors)
    {
        var options = CreateOptions(path, recoveryErrors);
        options.RootJournal = root.Journal;
        return new StorageEnvironment(options);
    }

    private static StorageEnvironmentOptions CreateOptions(string path, List<string> recoveryErrors)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(path);
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = 4 * 4096; // the header record + 3 slots, so each 8KB transaction rolls the journal
        options.CompressTxAboveSizeInBytes = long.MaxValue;
        options.OnRecoveryError += (_, e) => recoveryErrors.Add(e.Message);
        return options;
    }
}
