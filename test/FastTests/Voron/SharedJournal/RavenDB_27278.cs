using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.SharedJournal;

// RavenDB-27278 (RavenDB-24520 finding): recovery of a shared journal used to hash-validate every
// transaction before the owner filter, so one branch's corrupted transaction failed the root and every
// sibling hard-linked to that file. Foreign transactions are now skipped before validation, and the skip
// is rejected unless it lands on a following transaction header, so a corrupted size still fails loudly.
public class RavenDB_27278(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenFact(RavenTestCategory.Voron)]
    public void CorruptedTxOfOneBranchMustNotFailRecoveryOfRootAndSiblingBranch()
    {
        var setup = PrepareSharedJournalWithVictimTx();

        // flip one payload byte: the header (incl. JournalId) stays readable, only the hash check fails
        using (var fs = new FileStream(setup.JournalFile, FileMode.Open, FileAccess.ReadWrite))
        {
            fs.Position = setup.Victim.Offset + TransactionHeader.SizeOf;
            int payloadByte = fs.ReadByte();
            fs.Position = setup.Victim.Offset + TransactionHeader.SizeOf;
            fs.WriteByte((byte)(payloadByte ^ 0xFF));
        }

        using var rootOptions = StorageEnvironmentOptions.ForPathForTests(setup.RootPath);
        rootOptions.ManualFlushing = true;
        rootOptions.ManualSyncing = true;
        rootOptions.OnRecoveryError += (_, _) => { }; // subscribed like the server does

        using var root = new StorageEnvironment(rootOptions);
        using var _ = root.Journal.SharedJournalsScope();

        using (var rootTx = root.ReadTransaction())
        {
            Assert.Equal("yes", rootTx.ReadTree("rootTree").Read("root").Reader.ToString());
        }

        // before the fix this threw InvalidJournalException: B hash-validated A's corrupted transaction
        using (var branchB = OpenBranch(setup.BranchBPath, root))
        using (var tx = branchB.ReadTransaction())
        {
            Tree tree = tx.ReadTree("treeB");
            Assert.True(tree != null, "branch B lost its 'treeB' tree entirely");
            var b1 = tree.Read("b1");
            Assert.True(b1 != null, "branch B lost its committed transaction b1");
            Assert.Equal("1", b1.Reader.ToString());
            var b2 = tree.Read("b2");
            Assert.True(b2 != null, "branch B lost its committed transaction b2 (written AFTER branch A's corrupted one)");
            Assert.Equal("2", b2.Reader.ToString());
        }

        // the owner keeps failing loudly
        Assert.Throws<InvalidJournalException>(() =>
        {
            using var branchA = OpenBranch(setup.BranchAPath, root);
        });
    }

    [RavenFact(RavenTestCategory.Voron)]
    public unsafe void CorruptedSizeOfForeignTransactionMustFailLoudlyNotSilentlyLoseData()
    {
        var setup = PrepareSharedJournalWithVictimTx();

        // the size field is not covered by the transaction hash; an in-bounds garbage size pointing past
        // every later transaction must not be trusted for a skip (it would jump B over its own b2)
        const int garbageSize = 400 * 1024;
        long fileLength = new FileInfo(setup.JournalFile).Length;
        Assert.True(setup.Victim.Offset + TransactionHeader.SizeOf + garbageSize < fileLength,
            "garbage size must stay within the journal file, otherwise the existing bounds check rejects it before the skip is even considered");
        Assert.True(setup.Victim.Offset + garbageSize > setup.Txs[^1].Offset,
            "the garbage jump target must lie past every later transaction, so trusting it would silently swallow them");

        var bytes = File.ReadAllBytes(setup.JournalFile);
        fixed (byte* p = bytes)
        {
            var header = (TransactionHeader*)(p + setup.Victim.Offset);
            Assert.NotEqual(-1, (long)header->CompressedSize);
            header->CompressedSize = garbageSize;
        }
        File.WriteAllBytes(setup.JournalFile, bytes);

        using var rootOptions = StorageEnvironmentOptions.ForPathForTests(setup.RootPath);
        rootOptions.ManualFlushing = true;
        rootOptions.ManualSyncing = true;
        rootOptions.OnRecoveryError += (_, _) => { };

        using var root = new StorageEnvironment(rootOptions);
        using var _ = root.Journal.SharedJournalsScope();

        using (var rootTx = root.ReadTransaction())
        {
            Assert.Equal("yes", rootTx.ReadTree("rootTree").Read("root").Reader.ToString());
        }

        try
        {
            using var branchB = OpenBranch(setup.BranchBPath, root);
            using var tx = branchB.ReadTransaction();
            var b2 = tx.ReadTree("treeB")?.Read("b2");
            Assert.True(false, b2 == null
                ? "branch B opened cleanly and silently lost its committed transaction b2 - the corrupted foreign size field was trusted"
                : "branch B opened cleanly with all its data although the corrupted size should have failed its replay");
        }
        catch (InvalidJournalException)
        {
            // expected
        }

        Assert.Throws<InvalidJournalException>(() =>
        {
            using var branchA = OpenBranch(setup.BranchAPath, root);
        });
    }

    private sealed class Setup
    {
        public string RootPath, BranchAPath, BranchBPath, JournalFile;
        public Guid RootId, AId, BId;
        public List<(long Offset, Guid JournalId, long TxId)> Txs;
        public (long Offset, Guid JournalId, long TxId) Victim;
    }

    // root + branches A and B share one hard-linked journal file, nothing flushed or synced, so every
    // environment fully replays it on startup. File order: root txs, A boot, link record, B boot,
    // b1(B), victim(A), b2(B), a2(A)
    private Setup PrepareSharedJournalWithVictimTx()
    {
        var setup = new Setup
        {
            RootPath = NewDataPath(suffix: "-root"),
            BranchAPath = NewDataPath(suffix: "-branchA"),
            BranchBPath = NewDataPath(suffix: "-branchB"),
        };
        IOExtensions.DeleteDirectory(setup.RootPath);
        IOExtensions.DeleteDirectory(setup.BranchAPath);
        IOExtensions.DeleteDirectory(setup.BranchBPath);

        {
            using var rootOptions = StorageEnvironmentOptions.ForPathForTests(setup.RootPath);
            rootOptions.ManualFlushing = true;
            rootOptions.ManualSyncing = true;
            rootOptions.InitialLogFileSize = 1024 * 1024; // single physical journal file

            using var root = new StorageEnvironment(rootOptions);
            using var _ = root.Journal.SharedJournalsScope();

            using (var rootTx = root.WriteTransaction())
            {
                rootTx.CreateTree("rootTree").Add("root", "yes");
                rootTx.Commit();
            }

            var mre = new ManualResetEventSlim(false);
            root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);
            var task = Task.Run(() =>
            {
                var branchA = OpenBranch(setup.BranchAPath, root);
                var branchB = OpenBranch(setup.BranchBPath, root);

                using (var tx = branchB.WriteTransaction())
                {
                    tx.CreateTree("treeB").Add("b1", "1");
                    tx.Commit();
                }

                // the transaction the tests corrupt
                using (var tx = branchA.WriteTransaction())
                {
                    tx.CreateTree("treeA").Add("victim", "x");
                    tx.Commit();
                }

                // b2 after the victim - without it B would just truncate at the corruption point
                using (var tx = branchB.WriteTransaction())
                {
                    tx.CreateTree("treeB").Add("b2", "2");
                    tx.Commit();
                }

                // a2 keeps the owner failing loudly instead of truncating
                using (var tx = branchA.WriteTransaction())
                {
                    tx.CreateTree("treeA").Add("a2", "y");
                    tx.Commit();
                }

                return (branchA, branchB);
            });
            task.ContinueWith(_ => mre.Set());
            SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(task, mre, root);

            var (branchA, branchB) = task.Result;
            setup.RootId = root.HeaderAccessor.JournalId;
            setup.AId = branchA.HeaderAccessor.JournalId;
            setup.BId = branchB.HeaderAccessor.JournalId;

            branchB.Dispose();
            branchA.Dispose();
        }

        setup.JournalFile = Directory.GetFiles(Path.Combine(setup.BranchBPath, "Journals")).Single(); // same physical file for all three envs
        setup.Txs = ReadTransactions(File.ReadAllBytes(setup.JournalFile));

        Assert.NotEqual(setup.AId, setup.BId);
        var aTxs = setup.Txs.Where(t => t.JournalId == setup.AId).ToList();
        Assert.True(aTxs.Count >= 2, $"expected at least the victim + a2 transactions of branch A in {setup.JournalFile}, found {aTxs.Count}");
        setup.Victim = aTxs[^2];

        // both branches must have a later own tx after the victim; the root must not (mirrors production,
        // where the @SharedJournals root env has almost no transactions of its own)
        Assert.Contains(setup.Txs, t => t.Offset > setup.Victim.Offset && t.JournalId == setup.AId);
        Assert.Contains(setup.Txs, t => t.Offset > setup.Victim.Offset && t.JournalId == setup.BId);
        Assert.DoesNotContain(setup.Txs, t => t.Offset > setup.Victim.Offset && t.JournalId == setup.RootId);

        return setup;
    }

    private static StorageEnvironment OpenBranch(string branchPath, StorageEnvironment root)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(branchPath);
        options.RootJournal = root.Journal;
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.OnRecoveryError += (_, _) => { }; // subscribed like the server does
        return new StorageEnvironment(options);
    }

    private static unsafe List<(long Offset, Guid JournalId, long TxId)> ReadTransactions(byte[] journal)
    {
        var txs = new List<(long, Guid, long)>();
        fixed (byte* p = journal)
        {
            long pos = 0;
            while (pos + TransactionHeader.SizeOf <= journal.Length)
            {
                var header = (TransactionHeader*)(p + pos);
                if (header->HeaderMarker != Constants.TransactionHeaderMarker)
                {
                    pos += 4 * 1024;
                    continue;
                }

                txs.Add((pos, header->JournalId, header->TransactionId));

                long size = header->CompressedSize != -1 ? header->CompressedSize : header->UncompressedSize;
                long sizeIn4Kb = (size + sizeof(TransactionHeader)) / (4 * 1024) +
                                 ((size + sizeof(TransactionHeader)) % (4 * 1024) == 0 ? 0 : 1); // JournalReader.GetTransactionSizeIn4Kb
                pos += sizeIn4Kb * 4 * 1024;
            }
        }

        return txs;
    }
}
