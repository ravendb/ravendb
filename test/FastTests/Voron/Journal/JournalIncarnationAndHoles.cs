using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Raven.Server.Utils;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Util;
using Xunit;

namespace FastTests.Voron.Journal;

// A journal is a run of valid entries (of several environments, when shared) followed by the end of the live data. An
// entry left over from an earlier incarnation of a recycled file is not valid, so the recycled tail is just the end.
// Any valid entry after an invalid region is a hole: the file is closed for writes, and each environment decides on
// its own transactions - a hole that the durability watermark explains ends the replay, anything else is a hard error.
public class JournalIncarnationAndHoles(ITestOutputHelper output) : RavenTestBase(output)
{
    private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer(32);

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public unsafe void EntryOfAnEarlierIncarnationIsNotValidAndDoesNotMoveTheWritePosition(bool encrypted)
    {
        var path = NewDataPath();
        var (journalFile, journalNumber, journalId) = WriteTransactions(path, count: 3, encrypted);

        var bytes = File.ReadAllBytes(journalFile);
        var (maybeIncarnation, entries) = ReadEntries(bytes, journalId);
        var incarnation = maybeIncarnation!.Value;
        var own = entries.Where(e => e.JournalId == journalId).ToList();
        var lastLive = entries[^1];
        long liveEnd4Kb = lastLive.Offset / (4 * 1024) + lastLive.SizeIn4Kb;

        // the tail of a recycled file: a copy of one of our entries, as a previous incarnation of the file stamped it
        var previousIncarnation = Guid.NewGuid();
        long staleOffset = liveEnd4Kb * 4 * 1024;
        var source = own[^1];
        Assert.True(staleOffset + source.SizeIn4Kb * 4 * 1024 <= bytes.Length, "the journal must have room for the stale entry after the live data");
        Array.Copy(bytes, source.Offset, bytes, staleOffset, source.SizeIn4Kb * 4 * 1024);
        fixed (byte* p = bytes)
        {
            var stale = (TransactionHeader*)(p + staleOffset);
            stale->JournalId = stale->JournalId.Xor(incarnation).Xor(previousIncarnation);
            stale->Hash ^= TransactionHeader.IncarnationTag(incarnation) ^ TransactionHeader.IncarnationTag(previousIncarnation);
        }
        File.WriteAllBytes(journalFile, bytes);

        var recoveryErrors = new List<string>();
        using var options = CreateOptions(path, recoveryErrors, encrypted);
        using var env = new StorageEnvironment(options);

        Assert.Empty(recoveryErrors);
        AssertKeys(env, present: ["k1", "k2", "k3"], missing: []);

        // the recycled tail is not a hole: the file stays writable, and writes resume right after our last entry
        Assert.Equal(journalNumber, env.CurrentStateRecord.Journal.Number);
        Assert.Equal(liveEnd4Kb, env.CurrentStateRecord.Journal.Last4KWritePosition);
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void HoleExplainedByTheWatermarkEndsTheReplayAndClosesTheFile(bool encrypted)
    {
        var path = NewDataPath();
        var (journalFile, journalNumber, journalId) = WriteTransactions(path, count: 6, encrypted);

        var bytes = File.ReadAllBytes(journalFile);
        var own = ReadEntries(bytes, journalId).Entries.Where(e => e.JournalId == journalId).ToList();
        var missing = own[^2];
        var after = own[^1];
        Assert.Equal(missing.TxId + 1, after.TxId);

        CorruptPayload(bytes, missing);
        // submitted while the missing transaction was still in flight
        SetDurableTxIdDelta(bytes, after, 2);
        File.WriteAllBytes(journalFile, bytes);

        using var options = CreateOptions(path, recoveryErrors: [], encrypted);
        using (var env = new StorageEnvironment(options))
        {
            AssertKeys(env, present: ["k1", "k2", "k3", "k4"], missing: ["k5", "k6"]);

            using (var tx = env.WriteTransaction())
            {
                tx.CreateTree("t").Add("k7", "v");
                tx.Commit();
            }

            Assert.True(env.CurrentStateRecord.Journal.Number > journalNumber, "a journal with a hole must not take further writes");
        }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void TransactionSubmittedAfterTheMissingOneBecameDurableIsAHardError(bool encrypted)
    {
        var path = NewDataPath();
        var (journalFile, _, journalId) = WriteTransactions(path, count: 7, encrypted);

        var bytes = File.ReadAllBytes(journalFile);
        var own = ReadEntries(bytes, journalId).Entries.Where(e => e.JournalId == journalId).ToList();
        var missing = own[^3];
        var inFlight = own[^2];
        var later = own[^1];

        CorruptPayload(bytes, missing);
        // the first transaction after the hole explains it, but the next one was submitted once the missing one was durable
        SetDurableTxIdDelta(bytes, inFlight, 2);
        SetDurableTxIdDelta(bytes, later, 1);
        File.WriteAllBytes(journalFile, bytes);

        using var options = CreateOptions(path, recoveryErrors: [], encrypted);
        Assert.Throws<InvalidJournalException>(() =>
        {
            using var env = new StorageEnvironment(options);
        });
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void HoleWithoutAWatermarkIsAHardError(bool encrypted)
    {
        var path = NewDataPath();
        var (journalFile, _, journalId) = WriteTransactions(path, count: 6, encrypted);

        var bytes = File.ReadAllBytes(journalFile);
        var own = ReadEntries(bytes, journalId).Entries.Where(e => e.JournalId == journalId).ToList();

        CorruptPayload(bytes, own[^2]);
        SetDurableTxIdDelta(bytes, own[^1], 0);
        File.WriteAllBytes(journalFile, bytes);

        using var options = CreateOptions(path, recoveryErrors: [], encrypted);
        Assert.Throws<InvalidJournalException>(() =>
        {
            using var env = new StorageEnvironment(options);
        });
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void InvalidTransactionAtTheEndOfAJournalWithOurTransactionsInALaterOneRecoversPartiallyAndKeepsTheLaterJournals(bool encrypted)
    {
        var path = NewDataPath();
        var (journals, _) = WriteTransactionsOverSeveralJournals(path, encrypted);

        // a journal is switched only once all its writes completed, so its last transaction was acknowledged
        var (file, entries) = journals[0];
        var bytes = File.ReadAllBytes(file);
        CorruptPayload(bytes, entries[^1]);
        File.WriteAllBytes(file, bytes);

        AssertPartialRecoveryKeepsTheLaterJournals(path, journals, encrypted);
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void PipeliningHoleWithOurTransactionsInALaterJournalRecoversPartiallyAndKeepsTheLaterJournals(bool encrypted)
    {
        var path = NewDataPath();
        var (journals, journalId) = WriteTransactionsOverSeveralJournals(path, encrypted);

        var (file, entries) = journals[0];
        var own = entries.Where(e => e.JournalId == journalId).ToList();
        Assert.True(own.Count >= 3, $"the first journal must hold enough of our transactions, got {own.Count}");
        Assert.Equal(own[^2].TxId + 1, own[^1].TxId);

        // an explained hole at the end of the journal - but the next journal can only exist once all its writes completed
        var bytes = File.ReadAllBytes(file);
        CorruptPayload(bytes, own[^2]);
        SetDurableTxIdDelta(bytes, own[^1], 2);
        File.WriteAllBytes(file, bytes);

        AssertPartialRecoveryKeepsTheLaterJournals(path, journals, encrypted);
    }

    private void AssertPartialRecoveryKeepsTheLaterJournals(string path, List<(string File, List<Entry> Entries)> journals, bool encrypted)
    {
        var recoveryErrors = new List<string>();
        using (var options = CreateOptions(path, recoveryErrors, encrypted, SmallJournals))
        using (var env = new StorageEnvironment(options))
        {
            AssertKeys(env, present: ["k1"], missing: [$"k{TransactionsOverSeveralJournals}"]);

            Assert.Contains(recoveryErrors, e => e.Contains("Database recovered partially") && e.Contains("hold later transactions of this environment"));

            foreach (var (later, _) in journals.Skip(1))
            {
                Assert.False(File.Exists(later), $"{later} must not be recovered or reused");
                Assert.True(File.Exists(later + StorageEnvironmentOptions.UnrecoveredJournalSuffix), $"{later} holds acknowledged transactions and must be kept");
            }

            using var tx = env.WriteTransaction();
            tx.CreateTree("t").Add("after", "v");
            tx.Commit();
        }

        // the kept journals are not journals any more, and our transactions written after the partial recovery continue the
        // sequence where it stopped - the next recovery applies them and reports no partial recovery
        recoveryErrors.Clear();
        using (var options = CreateOptions(path, recoveryErrors, encrypted, SmallJournals))
        using (var env = new StorageEnvironment(options))
        {
            Assert.DoesNotContain(recoveryErrors, e => e.Contains("recovered partially"));
            AssertKeys(env, present: ["k1", "after"], missing: []);
        }
    }

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false)]
    [InlineData(true)]
    public void InvalidTransactionAtTheEndOfTheLastJournalIsATornTail(bool encrypted)
    {
        var path = NewDataPath();
        var (journals, _) = WriteTransactionsOverSeveralJournals(path, encrypted);

        var (file, entries) = journals[^1];
        var bytes = File.ReadAllBytes(file);
        CorruptPayload(bytes, entries[^1]);
        File.WriteAllBytes(file, bytes);

        using var options = CreateOptions(path, recoveryErrors: [], encrypted, SmallJournals);
        using var env = new StorageEnvironment(options);

        AssertKeys(env, present: ["k1"], missing: [$"k{TransactionsOverSeveralJournals}"]);
    }

    private const long SmallJournals = 256 * 1024;
    private const int TransactionsOverSeveralJournals = 24;

    private (List<(string File, List<Entry> Entries)> Journals, Guid JournalId) WriteTransactionsOverSeveralJournals(string path, bool encrypted)
    {
        IOExtensions.DeleteDirectory(path);

        Guid journalId;
        using (var options = CreateOptions(path, recoveryErrors: [], encrypted, SmallJournals))
        using (var env = new StorageEnvironment(options))
        {
            var value = new byte[24 * 1024];
            for (int i = 1; i <= TransactionsOverSeveralJournals; i++)
            {
                Random.Shared.NextBytes(value); // incompressible, so the transactions fill the small journals
                using var tx = env.WriteTransaction();
                tx.CreateTree("t").Add("k" + i, new MemoryStream(value));
                tx.Commit();
            }

            journalId = env.HeaderAccessor.JournalId;
        }

        var journals = Directory.GetFiles(Path.Combine(path, "Journals"), "*.journal")
            .Order()
            .Select(f => (File: f, ReadEntries(File.ReadAllBytes(f), journalId).Entries))
            .Where(j => j.Entries.Count > 0)
            .ToList();
        Assert.True(journals.Count >= 2, $"the transactions must span several journals, got {journals.Count}");
        return (journals, journalId);
    }

    private (string JournalFile, long JournalNumber, Guid JournalId) WriteTransactions(string path, int count, bool encrypted)
    {
        IOExtensions.DeleteDirectory(path);

        using var options = CreateOptions(path, recoveryErrors: [], encrypted);
        using var env = new StorageEnvironment(options);

        for (int i = 1; i <= count; i++)
        {
            using var tx = env.WriteTransaction();
            tx.CreateTree("t").Add("k" + i, "v");
            tx.Commit();
        }

        var journalNumber = env.CurrentStateRecord.Journal.Number;
        return (Path.Combine(path, "Journals", StorageEnvironmentOptions.JournalName(journalNumber)), journalNumber, env.HeaderAccessor.JournalId);
    }

    private StorageEnvironmentOptions CreateOptions(string path, List<string> recoveryErrors, bool encrypted, long maxLogFileSize = 1024 * 1024)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(path);
        options.ManualFlushing = true; // nothing reaches the data file, so recovery replays every transaction from the journal
        options.ManualSyncing = true;
        options.MaxLogFileSize = maxLogFileSize;
        options.InitialLogFileSize = maxLogFileSize;
        options.OnRecoveryError += (_, e) => recoveryErrors.Add(e.Message);
        if (encrypted)
            options.Encryption.MasterKey = _masterKey.ToArray();
        return options;
    }

    private static void AssertKeys(StorageEnvironment env, string[] present, string[] missing)
    {
        using var tx = env.ReadTransaction();
        var tree = tx.ReadTree("t");
        foreach (var key in present)
            Assert.True(tree.Read(key) != null, $"{key} must survive the recovery");
        foreach (var key in missing)
            Assert.True(tree.Read(key) == null, $"{key} must be discarded by the recovery");
    }

    private static void CorruptPayload(byte[] bytes, Entry entry)
    {
        bytes[entry.Offset + TransactionHeader.SizeOf] ^= 0xFF;
    }

    // not covered by the hash or the MAC, so it can be changed in place
    private static unsafe void SetDurableTxIdDelta(byte[] bytes, Entry entry, byte delta)
    {
        fixed (byte* p = bytes)
            ((TransactionHeader*)(p + entry.Offset))->DurableTxIdDeltaAtSubmit = delta;
    }

    private sealed record Entry(long Offset, long SizeIn4Kb, Guid JournalId, long TxId);

    // The environment is alone in its journal, so its first entry gives the incarnation (the payload of the journal
    // header record that holds it is encrypted when the environment is)
    private static unsafe (Guid? Incarnation, List<Entry> Entries) ReadEntries(byte[] journal, Guid journalId)
    {
        var entries = new List<Entry>();
        Guid? incarnation = null;
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

                if ((header->Flags & TransactionPersistenceModeFlags.JournalHeaderRecord) != 0)
                {
                    pos += 4 * 1024;
                    continue;
                }

                incarnation ??= header->JournalId.Xor(journalId);

                long size = (header->CompressedSize != -1 ? header->CompressedSize : header->UncompressedSize) + TransactionHeader.SizeOf;
                long sizeIn4Kb = (size + 4 * 1024 - 1) / (4 * 1024);
                entries.Add(new Entry(pos, sizeIn4Kb, header->JournalId.Xor(incarnation!.Value), header->TransactionId));
                pos += sizeIn4Kb * 4 * 1024;
            }
        }

        return (incarnation, entries);
    }
}
