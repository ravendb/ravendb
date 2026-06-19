using System.IO.Compression;
using Raven.Server.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Data.BTrees;
using Xunit;

namespace FastTests.Voron.SharedJournal;

public class RavenDB_26830(ITestOutputHelper output) : RavenTestBase(output)
{
    /*
     RavenDB_26830-legacy.zip is a Voron env generated with the RavenDB 6.2 binary (test/Tryouts).
     Its LAST journal is a *reused* (recycled) journal file: 6.2 only File.Move's synced journals into
     the reuse pool, never zeroing them, so the file keeps its old transaction bytes. After reuse the
     new (higher-id) head is written at the front, leaving the older (lower-id) transactions intact in
     the tail. Layout of the last journal (all tx headers have JournalId = Guid.Empty - legacy):

       blk 0 : txId 65   ] newer head (in-sequence, already synced)
       blk 2 : txId 66   ]
       blk 3+: <mid old-tx, no marker> -> invalid gap
       blk 4 : txId 51   ] older "recycled" tail
       blk 6 : txId 52   ]  ...

     Before the fix, v8.0 recovery read the head, hit the invalid gap, then VerifyNoUnexpectedValidTransactionsAfter
     found the older valid tx and threw InvalidJournalException - even though the main read loop (TryReadAndValidateHeader)
     skips that same recycled-legacy tx. It only surfaced with IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = false
     (the Voron/test default; the server default is true, which masked it in production).

     NOTE: found while investigating RavenDB-26830, but this is a SEPARATE, flag-masked regression - NOT the reported
     26830 "journal's last tx id is -1" (SetLastFlushed) failure, which is an unconditional, different path. A Voron
     snapshot/full-backup also cannot carry this shape: the backup copies the current journal only up to its write
     position, dropping the recycled tail.
    */
    [RavenFact(RavenTestCategory.Voron)]
    public void CanLoadLegacy62DbWithReusedJournalStaleTail()
    {
        string newDataPath = NewDataPath();
        IOExtensions.DeleteDirectory(newDataPath);
        using var stream = typeof(RavenDB_26830).Assembly.GetManifestResourceStream(typeof(RavenDB_26830).Namespace + ".RavenDB_26830-legacy.zip");
        using var zipArchive = new ZipArchive(stream, ZipArchiveMode.Read);
        zipArchive.ExtractToDirectory(newDataPath);

        var options = StorageEnvironmentOptions.ForPathForTests(newDataPath);
        options.ManualFlushing = true;
        // pin the path the fix addresses: recovery must skip the recycled-legacy tail even with this off.
        // server default is true; ForPathForTests default is false - set explicitly so the test stays meaningful
        // if the Voron default ever changes.
        options.IgnoreDataIntegrityErrorsOfAlreadySyncedTransactions = false;

        // before the fix this throws Voron.Exceptions.InvalidJournalException
        using var env = new StorageEnvironment(options);

        using (var txr = env.ReadTransaction())
        {
            Tree tree = txr.ReadTree("legacy-tree");
            Assert.NotNull(tree);
            Assert.True(tree.ReadHeader().NumberOfEntries >= 16);
        }

        // the recovered env must stay writable
        using (var txw = env.WriteTransaction())
        {
            txw.ReadTree("legacy-tree").Add("after-recovery", "works");
            txw.Commit();
        }

        using (var txr = env.ReadTransaction())
        {
            Tree tree = txr.ReadTree("legacy-tree");
            Assert.Equal("works", tree.Read("after-recovery").Reader.ToStringValue());
        }
    }
}

/*
================================================================================================
How RavenDB_26830-legacy.zip was generated (run with the RavenDB 6.2 binary, NOT 8.0)
================================================================================================
The fixture cannot be produced by 8.0: 8.0 stamps a real JournalId on every transaction and its
recovery/recycling differ. It also cannot be produced by a snapshot backup: Voron's FullBackup
copies the current journal only up to its write head (pagesToCopy = lastWrittenLogPage + 1),
dropping the stale recycled tail. So it must be an in-place 6.2 data dir.

Reproduce: drop this Program.cs into 6.2 `test/Tryouts`, build, and run:
    Tryouts.exe <outDir> 6000 8 60
then zip the CONTENTS of <outDir>/db (Raven.voron, headers.one/two, Journals/, Temp/) at the
archive root. Idea: churn so journals fill and get pooled for reuse (6.2 only File.Move's synced
journals into the pool, never zeroing them); force a roll into a pooled old-content file; then
write ONE tiny tx so the head ends one block into the next old tx -> the invalid gap that v8 trips on.

using System;
using System.IO;
using System.Threading;
using Voron;
using Voron.Global;

namespace Tryouts;

public static class Program
{
    public static void Main(string[] args)
    {
        string root = args.Length > 0 ? args[0] : @"D:\workspace\_legacy62_gen";
        int bigValueSize = args.Length > 1 ? int.Parse(args[1]) : 6000;  // -> multi-block old txs
        int journalBlocks = args.Length > 2 ? int.Parse(args[2]) : 16;   // journal size in 8KB pages
        int phase1 = args.Length > 3 ? int.Parse(args[3]) : 120;

        string dbDir = Path.Combine(root, "db");
        if (Directory.Exists(root))
            Directory.Delete(root, true);
        Directory.CreateDirectory(dbDir);
        string journalsDir = Path.Combine(dbDir, "Journals");

        var options = StorageEnvironmentOptions.ForPath(dbDir);
        options.ManualFlushing = true;
        options.MaxLogFileSize = journalBlocks * Constants.Storage.PageSize;

        var r = new Random(1);
        var big = new byte[bigValueSize];
        int tx = 0;

        using (var env = new StorageEnvironment(options))
        {
            // Phase 1: churn so several journals fill, then sync so they are pooled for reuse
            // WITH THEIR OLD CONTENT INTACT (6.2 only File.Move's them, never zeroes).
            for (int i = 0; i < phase1; i++, tx++)
            {
                using (var w = env.WriteTransaction())
                {
                    r.NextBytes(big);
                    w.CreateTree("legacy-tree").Add($"item/{tx % 16}", new MemoryStream(big)); // reuse keys -> tiny data file
                    w.Commit();
                }
            }

            env.FlushLogToDataFile();
            env.ForceSyncDataFile();

            bool pooled = SpinWait.SpinUntil(() => env.Options.GetNumberOfJournalsForReuse() >= 3, TimeSpan.FromSeconds(30));
            Console.WriteLine($"journals pooled for reuse: {env.Options.GetNumberOfJournalsForReuse()} (>=3: {pooled})");

            // Phase 2: keep writing same-size txs until the current journal ROLLS into a
            // pooled (old-content) file. The roll-triggering tx becomes the head's first tx.
            long before = MaxJournal(journalsDir);
            int guard = 0;
            while (MaxJournal(journalsDir) == before && guard++ < 100000)
            {
                using (var w = env.WriteTransaction())
                {
                    r.NextBytes(big);
                    w.CreateTree("legacy-tree").Add($"item/{tx % 16}", new MemoryStream(big));
                    w.Commit();
                }
                tx++;
            }
            Console.WriteLine($"rolled into reused journal {MaxJournal(journalsDir)} after {guard} extra writes");

            // One tiny tx so the head ends exactly ONE block into the next old tx ->
            // guarantees the invalid gap that v8 recovery trips on.
            using (var w = env.WriteTransaction())
            {
                w.CreateTree("legacy-tree").Add($"tail/{tx}", new MemoryStream(new byte[8]));
                w.Commit();
            }
            tx++;

            env.FlushLogToDataFile();
            env.ForceSyncDataFile();
        }

        Console.WriteLine($"\ntotal txs written: {tx}");
        Console.WriteLine("journals on disk after clean dispose:");
        foreach (var f in Directory.GetFiles(journalsDir))
            Console.WriteLine($"  {Path.GetFileName(f)}  {new FileInfo(f).Length:N0} bytes");

        ScanLastJournal(journalsDir);
        Console.WriteLine($"\nFIXTURE ENV DIR: {dbDir}");
    }

    static long MaxJournal(string journalsDir)
    {
        long max = -1;
        foreach (var f in Directory.GetFiles(journalsDir, "*.journal"))
        {
            if (long.TryParse(Path.GetFileNameWithoutExtension(f), out var n))
                max = Math.Max(max, n);
        }
        return max;
    }

    // confirm the [high-id head][gap][low-id tail] shape at 4KB boundaries
    static void ScanLastJournal(string journalsDir)
    {
        long num = MaxJournal(journalsDir);
        string path = Path.Combine(journalsDir, num.ToString("D19") + ".journal");
        byte[] data = File.ReadAllBytes(path);
        const int blk = 4096;
        Console.WriteLine($"\nscan of last journal {Path.GetFileName(path)} ({data.Length:N0} bytes) - tx header at each 4KB block:");
        long prev = long.MinValue;
        bool sawGapThenOlder = false;
        for (int off = 0; off + 16 <= data.Length; off += blk)
        {
            ulong marker = BitConverter.ToUInt64(data, off);
            if (marker != Constants.TransactionHeaderMarker)
                continue;
            long txId = BitConverter.ToInt64(data, off + 8);
            string note = "";
            if (prev != long.MinValue && txId < prev)
            {
                note = "  <== OLDER tx after newer (recycled tail)";
                sawGapThenOlder = true;
            }
            Console.WriteLine($"  off {off,9} (blk {off / blk,4}): txId={txId}{note}");
            prev = Math.Max(prev, txId);
        }
        Console.WriteLine(sawGapThenOlder
            ? "SHAPE OK: journal contains an older 'recycled' tx after the newer head."
            : "SHAPE MISSING: no older-after-newer tx found - tune sizes.");
    }
}
================================================================================================
*/
