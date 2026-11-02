using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Util;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class PipeliningHoleCrashLoop(ITestOutputHelper output) : RavenTestBase(output)
    {
        [RavenFact(RavenTestCategory.Voron)]
        public void SecondCrashWithAnInFlightHoleInTheNextJournalIsNotAnAcknowledgedLoss()
        {
            var path = NewDataPath();
            var recoveryErrors = new List<string>();
            Guid journalId;
            long firstJournal;

            using (var options = CreateOptions(path, recoveryErrors))
            using (var env = new StorageEnvironment(options))
            {
                for (int i = 1; i <= 6; i++)
                    Put(env, "k" + i);

                journalId = env.HeaderAccessor.JournalId;
                firstJournal = env.CurrentStateRecord.Journal.Number;
            }

            // crash 1: k5 was in flight and never made it, k6 was submitted while k5 was still in flight
            var firstFile = JournalPath(path, firstJournal);
            MakeInFlightHole(firstFile, journalId);

            long secondJournal;
            using (var options = CreateOptions(path, recoveryErrors))
            using (var env = new StorageEnvironment(options))
            {
                AssertKeys(env, present: ["k1", "k2", "k3", "k4"], missing: ["k5", "k6"]);

                Put(env, "a1");
                Put(env, "a2");

                secondJournal = env.CurrentStateRecord.Journal.Number;
                Assert.True(secondJournal > firstJournal, "a journal with a hole must not take further writes");
            }

            Assert.True(File.Exists(firstFile), "nothing was flushed, so the journal with the first hole is still there");

            // crash 2: a1 was in flight and never made it, a2 was submitted while a1 was still in flight
            var secondFile = JournalPath(path, secondJournal);
            MakeInFlightHole(secondFile, journalId);

            recoveryErrors.Clear();
            using (var options = CreateOptions(path, recoveryErrors))
            using (var env = new StorageEnvironment(options))
            {
                Assert.DoesNotContain(recoveryErrors, e => e.Contains("recovered partially"));
                Assert.False(File.Exists(secondFile + ".unrecovered"), "the second journal holds nothing acknowledged, it must not be kept as lost data");
                AssertKeys(env, present: ["k1", "k2", "k3", "k4"], missing: ["k5", "k6", "a1", "a2"]);
            }
        }

        private static void MakeInFlightHole(string file, Guid journalId)
        {
            var bytes = File.ReadAllBytes(file);
            var own = ReadOwnEntries(bytes, journalId);
            Assert.True(own.Count >= 2, $"{file} must hold at least two of our transactions, got {own.Count}");
            var missing = own[^2];
            var after = own[^1];
            Assert.Equal(missing.TxId + 1, after.TxId);

            bytes[missing.Offset + TransactionHeader.SizeOf] ^= 0xFF;
            SetDurableTxIdDelta(bytes, after, 2); // W = after - 2 = missing - 1, i.e. submitted while the missing one was in flight
            File.WriteAllBytes(file, bytes);
        }

        private static void Put(StorageEnvironment env, string key)
        {
            using var tx = env.WriteTransaction();
            tx.CreateTree("t").Add(key, "v");
            tx.Commit();
        }

        private static string JournalPath(string path, long number) => Path.Combine(path, "Journals", StorageEnvironmentOptions.JournalName(number));

        private static StorageEnvironmentOptions CreateOptions(string path, List<string> recoveryErrors)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(path);
            options.ManualFlushing = true;
            options.ManualSyncing = true;
            options.MaxLogFileSize = 1024 * 1024;
            options.InitialLogFileSize = 1024 * 1024;
            options.OnRecoveryError += (_, e) => recoveryErrors.Add(e.Message);
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

        private static unsafe void SetDurableTxIdDelta(byte[] bytes, Entry entry, byte delta)
        {
            fixed (byte* p = bytes)
                ((TransactionHeader*)(p + entry.Offset))->DurableTxIdDeltaAtSubmit = delta;
        }

        private sealed record Entry(long Offset, long TxId);

        // the environment is alone in its journal, so its first entry gives the incarnation
        private static unsafe List<Entry> ReadOwnEntries(byte[] journal, Guid journalId)
        {
            var entries = new List<(Entry Entry, Guid JournalId)>();
            Guid? incarnation = null;
            fixed (byte* p = journal)
            {
                long pos = 0;
                while (pos + TransactionHeader.SizeOf <= journal.Length)
                {
                    var header = (TransactionHeader*)(p + pos);
                    if (header->HeaderMarker != Constants.TransactionHeaderMarker ||
                        (header->Flags & TransactionPersistenceModeFlags.JournalHeaderRecord) != 0)
                    {
                        pos += 4 * 1024;
                        continue;
                    }

                    incarnation ??= header->JournalId.Xor(journalId);

                    long size = (header->CompressedSize != -1 ? header->CompressedSize : header->UncompressedSize) + TransactionHeader.SizeOf;
                    long sizeIn4Kb = (size + 4 * 1024 - 1) / (4 * 1024);
                    entries.Add((new Entry(pos, header->TransactionId), header->JournalId.Xor(incarnation.Value)));
                    pos += sizeIn4Kb * 4 * 1024;
                }
            }

            return entries.Where(e => e.JournalId == journalId).Select(e => e.Entry).ToList();
        }
    }
}
