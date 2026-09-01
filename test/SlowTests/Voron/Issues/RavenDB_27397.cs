using System;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Voron;
using FastTests.Voron.SharedJournal;
using Raven.Server.Utils;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Voron.Impl.Journal;
using Xunit;
using ITestOutputHelper = Xunit.ITestOutputHelper;

namespace SlowTests.Voron.Issues;

public class RavenDB_27397 : StorageTest
{
    public RavenDB_27397(ITestOutputHelper output) : base(output)
    {
    }

    protected override void Configure(StorageEnvironmentOptions options)
    {
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = 256 * 1024;
        options.MaxNumberOfRecyclableJournals = 32;
        // these tests assert exact recyclable-file counts - the background pool preparation would
        // add files of its own
        options.EnableJournalPoolPrewarming = false;
    }

    private string JournalPath => ((StorageEnvironmentOptions.DirectoryStorageEnvironmentOptions)Env.Options).JournalPath.FullPath;

    private string[] GetRecyclableJournalFiles(string journalPath = null)
    {
        return Directory.GetFiles(journalPath ?? JournalPath, $"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}.*");
    }

    private void WriteItems(int from, int count, int size = 4096, string tree = "items")
    {
        var r = new Random(42);
        var bytes = new byte[size];

        for (int i = from; i < from + count; i++)
        {
            using (var tx = Env.WriteTransaction())
            {
                r.NextBytes(bytes);
                bytes[0] = (byte)(i & 0xFF);
                bytes[1] = (byte)((i >> 8) & 0xFF);

                tx.CreateTree(tree).Add($"item/{i}", new MemoryStream(bytes));
                tx.Commit();
            }
        }
    }

    private void AssertItems(int upto, string tree = "items")
    {
        using (var tx = Env.ReadTransaction())
        {
            var t = tx.ReadTree(tree);
            for (int i = 0; i < upto; i++)
            {
                var result = t.Read($"item/{i}");
                Assert.NotNull(result);
                var buffer = new byte[2];
                result.Reader.Read(buffer, 0, 2);
                Assert.Equal(i & 0xFF, buffer[0]);
                Assert.Equal((i >> 8) & 0xFF, buffer[1]);
            }
        }
    }

    private void FlushAndSync()
    {
        Env.FlushLogToDataFile();

        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(Env.Journal.Applicator))
        {
            Assert.True(operation.SyncDataFile());
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Journals_are_recycled_after_sync_and_reused_for_new_journals()
    {
        RequireFileBasedPager();

        WriteItems(0, 200);

        Assert.Empty(GetRecyclableJournalFiles());

        FlushAndSync();

        Assert.True(SpinWait.SpinUntil(() => GetRecyclableJournalFiles().Length > 0, TimeSpan.FromSeconds(30)),
            "expected journals to land in the recycle pool after sync");

        var poolSize = GetRecyclableJournalFiles().Length;
        Assert.Equal(poolSize, Env.Options.GetNumberOfJournalsForReuse());

        // keep writing until a new journal file is created - it must come from the pool
        var journalsBefore = Directory.GetFiles(JournalPath, "*.journal").Length;
        var next = 200;
        while (Directory.GetFiles(JournalPath, "*.journal").Length == journalsBefore)
        {
            WriteItems(next, 10);
            next += 10;
            Assert.True(next < 2000, "no new journal file was created after many writes");
        }

        Assert.True(GetRecyclableJournalFiles().Length < poolSize,
            "a recycled journal should have been consumed for the new journal file");

        AssertItems(next);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Recovery_skips_stale_transactions_in_a_reused_journal()
    {
        RequireFileBasedPager();

        // fill several journals, then release them into the recycle pool
        WriteItems(0, 300);
        FlushAndSync();

        Assert.True(SpinWait.SpinUntil(() => GetRecyclableJournalFiles().Length > 0, TimeSpan.FromSeconds(30)));
        var poolSize = GetRecyclableJournalFiles().Length;

        // keep writing until a pool file is consumed for a new journal. The new journal keeps the
        // previous life's transactions past the new write position: valid checksums, same environment
        var next = 300;
        while (GetRecyclableJournalFiles().Length >= poolSize)
        {
            WriteItems(next, 10);
            next += 10;
            Assert.True(next < 2000, "no recycled journal was reused after many writes");
        }

        // a few more transactions on top of the reused journal, then recover. Without the journal
        // header record the recovery would trip over the stale transactions at the tail
        WriteItems(next, 5);
        next += 5;

        RestartDatabase();

        AssertItems(next);

        // the recovered env must stay usable across another cycle
        WriteItems(next, 20);
        next += 20;

        RestartDatabase();

        AssertItems(next);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Resurrected_previous_life_of_a_recycled_journal_never_corrupts()
    {
        // In normal operation this state cannot exist: the header record of a reused journal becomes
        // durable while the file still carries its recyclable name, and only then the file is renamed.
        // This test manually resurrects a pool file under a journal name (as a user restoring files by
        // hand might) and verifies there is no silent corruption: the content is fully synced by
        // construction, so recovery either walks it as already-synced transactions and succeeds, or
        // fails loudly on the transaction sequence gap

        RequireFileBasedPager();

        WriteItems(0, 300);
        FlushAndSync();

        Assert.True(SpinWait.SpinUntil(() => GetRecyclableJournalFiles().Length > 0, TimeSpan.FromSeconds(30)));

        // a couple of writes so the tail journal exists and holds data of this life
        WriteItems(300, 5);

        var journalPath = JournalPath;
        var recyclable = GetRecyclableJournalFiles().First();
        var staleContent = File.ReadAllBytes(recyclable);

        var latestJournal = Directory.GetFiles(journalPath, "*.journal")
            .Select(f => long.Parse(Path.GetFileNameWithoutExtension(f)))
            .Max();

        RestartDatabase(disposeOnly: true);

        var resurrected = Path.Combine(journalPath, StorageEnvironmentOptions.JournalName(latestJournal + 1));
        File.WriteAllBytes(resurrected, staleContent);

        try
        {
            StartDatabaseAfterDisposeOnly();
        }
        catch (InvalidJournalException)
        {
            // the loud path: recovery refuses the manually manipulated journal directory
            File.Delete(resurrected);
            StartDatabaseAfterDisposeOnly();
        }

        AssertItems(305);

        WriteItems(305, 20);

        RestartDatabase();

        AssertItems(325);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Hard_linked_journals_are_deleted_not_recycled()
    {
        RequireFileBasedPager();

        WriteItems(0, 300);

        // hard-link every journal file somewhere else - the delete path must see nlink > 1 and
        // unlink instead of recycling (writing to a recycled file that has another link would
        // corrupt the environment owning that link)
        var linkDir = Path.Combine(DataDir, "links");
        Directory.CreateDirectory(linkDir);

        var journals = Directory.GetFiles(JournalPath, "*.journal");
        foreach (var journal in journals)
        {
            var rc = Pal.rvn_hard_link_non_durable(journal, Path.Combine(linkDir, Path.GetFileName(journal)), out var errorCode);
            Assert.True(rc == PalFlags.FailCodes.Success, $"failed to create hard link: {rc}, errno: {errorCode}");
        }

        FlushAndSync();

        // journals released by the sync were all hard-linked - none may enter the recycle pool
        Assert.True(SpinWait.SpinUntil(() => Directory.GetFiles(JournalPath, "*.journal").Length < journals.Length, TimeSpan.FromSeconds(30)),
            "expected some journals to be released after sync");

        Assert.Empty(GetRecyclableJournalFiles());

        // the linked copies must be intact
        Assert.Equal(journals.Length, Directory.GetFiles(linkDir).Length);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Cleanup_removes_recycled_journals()
    {
        RequireFileBasedPager();

        WriteItems(0, 300);
        FlushAndSync();

        Assert.True(SpinWait.SpinUntil(() => GetRecyclableJournalFiles().Length > 0, TimeSpan.FromSeconds(30)));

        Env.Options.TryCleanupRecycledJournals();

        Assert.Empty(GetRecyclableJournalFiles());
        Assert.Equal(0, Env.Options.GetNumberOfJournalsForReuse());
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Corrupted_header_record_in_the_last_journal_loses_only_that_journal()
    {
        RequireFileBasedPager();

        // durable prefix
        WriteItems(0, 200);
        FlushAndSync();

        // unsynced suffix, spans several journals
        WriteItems(200, 60);

        var journalPath = JournalPath;
        var lastJournal = Directory.GetFiles(journalPath, "*.journal")
            .Select(f => long.Parse(Path.GetFileNameWithoutExtension(f)))
            .Max();

        RestartDatabase(disposeOnly: true);

        // wipe the journal header record (block 0) of the last journal: the incarnation is lost, so
        // every entry of that journal decodes to a foreign id and the journal contributes nothing
        CorruptFirst4Kb(Path.Combine(journalPath, StorageEnvironmentOptions.JournalName(lastJournal)));

        StartDatabaseAfterDisposeOnly();

        // the synced prefix must survive; the unsynced suffix may be cut, but only as a clean prefix -
        // a hole (a lost transaction followed by a recovered one) would mean silent corruption
        AssertItems(200);

        var lost = false;
        using (var tx = Env.ReadTransaction())
        {
            var t = tx.ReadTree("items");
            for (int i = 200; i < 260; i++)
            {
                var exists = t.Read($"item/{i}") != null;
                if (lost)
                    Assert.False(exists, $"item/{i} recovered after a lost transaction - the recovery has a hole");
                lost |= exists == false;
            }
        }

        Assert.True(lost, "expected the corrupted journal to lose its transactions");

        WriteItems(260, 20);

        RestartDatabase();

        AssertItems(200);
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Corrupted_header_record_in_a_middle_journal_fails_recovery_loudly()
    {
        RequireFileBasedPager();

        // unsynced transactions spanning several journals
        WriteItems(0, 200);

        var journalPath = JournalPath;
        var journals = Directory.GetFiles(journalPath, "*.journal")
            .Select(f => long.Parse(Path.GetFileNameWithoutExtension(f)))
            .OrderBy(n => n)
            .ToArray();

        Assert.True(journals.Length >= 3, "test needs at least three journals");
        var middleJournal = journals[journals.Length - 2];

        RestartDatabase(disposeOnly: true);

        // wiping the header record makes the whole journal decode as foreign, so the next journal's
        // transactions expose a sequence gap - the recovery must fail loudly, not lose data silently
        CorruptFirst4Kb(Path.Combine(journalPath, StorageEnvironmentOptions.JournalName(middleJournal)));

        Assert.Throws<InvalidJournalException>(StartDatabaseAfterDisposeOnly);
    }

    private static void CorruptFirst4Kb(string journalFile)
    {
        using (var fileStream = new FileStream(journalFile, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete))
        {
            var garbage = new byte[4096];
            Array.Fill(garbage, (byte)42);
            fileStream.Write(garbage, 0, garbage.Length);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void Pool_preparation_creates_a_zeroed_journal_and_the_next_roll_consumes_it()
    {
        RequireFileBasedPager();
        Options.EnableJournalPoolPrewarming = true;
        Env.WriteFlow.ForTestingPurposesOnly().ForceZeroedJournalPreparation = true;

        // a prepared file appears after the half-fill trigger fires, and every roll consumes it
        // again - so probe between small write batches, when no roll can take it away
        var next = 0;
        Assert.True(SpinWait.SpinUntil(() =>
        {
            if (GetRecyclableJournalFiles().Length > 0)
                return true;
            WriteItems(next, 2);
            next += 2;
            return GetRecyclableJournalFiles().Length > 0;
        }, TimeSpan.FromSeconds(60)), "the half-fill trigger did not prepare a pool file");

        var prepared = GetRecyclableJournalFiles()[0];
        var file = new FileInfo(prepared);
        Assert.True(file.Length >= 128 * 1024, $"prepared file is too small: {file.Length}");

        // fully written (zeroed), not sparse - the writes are what convert the extents
        Assert.True(GetAllocatedBytes(prepared) >= file.Length, "prepared file is sparse - the zeros were not written");

        // the next roll must take the prepared file instead of creating a fresh one
        var journalsBefore = Directory.GetFiles(JournalPath, "*.journal").Length;
        Assert.True(SpinWait.SpinUntil(() =>
        {
            WriteItems(next, 5);
            next += 5;
            return Directory.GetFiles(JournalPath, "*.journal").Length > journalsBefore;
        }, TimeSpan.FromSeconds(60)), "no journal roll happened");

        Assert.False(File.Exists(prepared), "the roll did not consume the prepared pool file");

        AssertItems(next);
    }

    private static long GetAllocatedBytes(string path)
    {
        var psi = new System.Diagnostics.ProcessStartInfo("du", $"-B1 \"{path}\"") { RedirectStandardOutput = true };
        using var proc = System.Diagnostics.Process.Start(psi);
        var line = proc.StandardOutput.ReadLine();
        proc.WaitForExit();
        return long.Parse(line.Split('\t')[0]);
    }

    private void RestartDatabase(bool disposeOnly)
    {
        // StorageTest.RestartDatabase disposes the options, which also removes the recycle pool
        // files - tests that need to manipulate journal files between stop and start use this pair
        Assert.True(disposeOnly);
        StopDatabase(shouldDisposeOptions: true);
    }

    private void StartDatabaseAfterDisposeOnly()
    {
        var manualFlush = Options.ManualFlushing;
        var manualSync = Options.ManualSyncing;

        Options = StorageEnvironmentOptions.ForPathForTests(DataDir);
        Options.ManualSyncing = manualSync;
        Options.ManualFlushing = manualFlush;
        Configure(Options);

        StartDatabase();
    }
}

public class RavenDB_27397_SharedJournals(ITestOutputHelper output) : RavenTestBase(output)
{
    /*
     In shared-journal mode only the ROOT creates journal files - the branches hard-link them. A pool
     held by a branch would therefore never be consumed, so a branch that happens to release the last
     link donates the file into the root's pool. Which environment donates is decided purely by who
     releases last: a branch lagging on flush / sync holds the last link of many files. This test pins
     that direction - the root releases its links first, so the branch is the last releaser.
    */
    [RavenFact(RavenTestCategory.Voron)]
    public void Branch_releasing_the_last_link_donates_the_journal_to_the_root_pool()
    {
        string rootPath = NewDataPath(suffix: "-root");
        string branchPath = NewDataPath(suffix: "-branch");
        IOExtensions.DeleteDirectory(rootPath);
        IOExtensions.DeleteDirectory(branchPath);

        using var rootOptions = StorageEnvironmentOptions.ForPathForTests(rootPath);
        rootOptions.ManualFlushing = true;
        rootOptions.ManualSyncing = true;
        rootOptions.MaxLogFileSize = 4096 * 8;
        rootOptions.EnableJournalPoolPrewarming = false; // the test asserts exact pool contents

        using var root = new StorageEnvironment(rootOptions);
        using var scope = root.Journal.SharedJournalsScope();

        var mre = new ManualResetEventSlim(false);
        root.Journal.BranchJournalMerger = new SharedJournalTests.MyJournalMerger(mre);

        var openBranch = Task.Run(() => SharedJournalTests.CreateBranchEnv(branchPath, root));
        openBranch.ContinueWith(_ => mre.Set());
        SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(openBranch, mre, root);

        using var branch = openBranch.Result;

        // fill several shared journals with branch transactions
        var writes = Task.Run(() =>
        {
            var bytes = new byte[2048];
            new Random(42).NextBytes(bytes);

            for (int i = 0; i < 40; i++)
            {
                using var tx = branch.WriteTransaction();
                tx.CreateTree("branchTree").Add($"item/{i}", new MemoryStream(bytes));
                tx.Commit();
            }
        });
        writes.ContinueWith(_ => mre.Set());
        SharedJournalTests.WaitForTaskAndExecuteBranchTransactions(writes, mre, root);

        string rootJournals = root.Options.JournalPath.FullPath;
        string branchJournals = branch.Options.JournalPath.FullPath;

        Assert.True(Directory.GetFiles(branchJournals, "*.journal").Length > 1, "expected the writes to span several shared journals");

        // the root releases its links FIRST - each file still has the branch's link, so nothing may
        // be recycled here (writing to a file another environment links to would corrupt it)
        root.FlushLogToDataFile();
        root.SyncDataFileImmediately();

        Assert.Empty(GetRecyclable(rootJournals));
        Assert.Empty(GetRecyclable(branchJournals));

        // now the branch releases - it holds the last link of those files, so they are recyclable,
        // and must land in the ROOT's pool (the only place they can be reused from)
        branch.FlushLogToDataFile();
        branch.SyncDataFileImmediately();

        Assert.True(SpinWait.SpinUntil(() => GetRecyclable(rootJournals).Length > 0, TimeSpan.FromSeconds(30)),
            "the branch released the last link of at least one journal - it should have been donated to the root's pool");

        Assert.Empty(GetRecyclable(branchJournals));
        Assert.Equal(GetRecyclable(rootJournals).Length, root.Options.GetNumberOfJournalsForReuse());
        Assert.Equal(0, branch.Options.GetNumberOfJournalsForReuse());

        // the donated files are usable: the root consumes them for its own new journals
        var beforeReuse = GetRecyclable(rootJournals).Length;
        var payload = new byte[4096];
        new Random(7).NextBytes(payload);

        for (int i = 0; i < 40 && GetRecyclable(rootJournals).Length >= beforeReuse; i++)
        {
            using var tx = root.WriteTransaction();
            tx.CreateTree("rootTree").Add($"item/{i}", new MemoryStream(payload));
            tx.Commit();
        }

        Assert.True(GetRecyclable(rootJournals).Length < beforeReuse, "the root should have consumed a donated journal for a new file");

        using (var tx = root.ReadTransaction())
            Assert.NotNull(tx.ReadTree("rootTree").Read("item/0"));

        using (var tx = branch.ReadTransaction())
            Assert.NotNull(tx.ReadTree("branchTree").Read("item/0"));
    }

    private static string[] GetRecyclable(string journalPath) =>
        Directory.GetFiles(journalPath, $"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}.*");
}
