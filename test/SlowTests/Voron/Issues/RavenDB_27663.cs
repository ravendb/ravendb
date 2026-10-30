using System;
using System.IO;
using System.Linq;
using System.Threading;
using FastTests;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Journal;
using Xunit;

namespace SlowTests.Voron.Issues;

public class RavenDB_27663(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int JournalSize = 256 * 1024;

    // a reused journal is the renamed pool file, so it keeps the creation time; a journal created from scratch gets the current one
    private static readonly DateTime Sentinel = new(2000, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    // the zeroed pool journal is <dir>\recyclable-journal.<19 digits>.tmp, 43 chars more than the dir: 259 chars for 216, 260 for 217.
    // A dir under 260 chars gets no \\?\ prefix, while the journals themselves (28 chars more) still fit in MAX_PATH.
    [RavenMultiplatformTheory(RavenTestCategory.Voron, RavenPlatform.Windows)]
    [InlineData(216)]
    [InlineData(217)]
    public void HalfFullJournalPreparesAPoolJournal(int journalDirLength)
    {
        using var options = CreateOptions(NewDataDir(journalDirLength), journalDirLength);
        options.EnableJournalPoolPrewarming = true;
        using var prepared = new ManualResetEventSlim();
        options.ForTestingPurposesOnly().AfterJournalZeroing = prepared.Set;

        using var env = new StorageEnvironment(options);
        env.WriteFlow.ForTestingPurposesOnly().ForceZeroedJournalPreparation = true;

        for (int i = 0; i < 64 && prepared.IsSet == false; i++)
        {
            Write(env, "k" + i);
            prepared.Wait(TimeSpan.FromSeconds(1));
        }

        Assert.True(prepared.IsSet, "a half-full journal should start a pool journal preparation");
        Assert.Equal(1, options.GetNumberOfJournalsForReuse());
    }

    // the pool journal is <dir>\recyclable-journal.<19 digits>, 39 chars more than the dir: 259 chars for 220, 260 for 221
    [RavenMultiplatformTheory(RavenTestCategory.Voron, RavenPlatform.Windows)]
    [InlineData(220)]
    [InlineData(221)]
    public void RecycledJournalIsReusedForTheNextJournal(int journalDirLength)
    {
        using var options = CreateOptions(NewDataDir(journalDirLength), journalDirLength);
        using var env = new StorageEnvironment(options);

        int written = 0;
        while (written < 40)
            Write(env, "k" + written++);

        env.FlushLogToDataFile();
        using (var sync = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
            Assert.True(sync.SyncDataFile());

        Assert.True(SpinWait.SpinUntil(() => GetPoolFiles(options).Length > 0, TimeSpan.FromSeconds(30)), "the synced journals should land in the pool");

        int journalsBefore = GetJournals(options).Length;
        for (int i = 0; GetJournals(options).Length == journalsBefore; i++)
        {
            Assert.True(i < 100, "no journal roll happened");

            foreach (string poolFile in GetPoolFiles(options))
                File.SetCreationTimeUtc(poolFile, Sentinel);

            Write(env, "k" + written++);
        }

        Assert.Equal(Sentinel, File.GetCreationTimeUtc(GetJournals(options).Max(StringComparer.Ordinal)));
    }

    [RavenMultiplatformTheory(RavenTestCategory.Voron, RavenPlatform.Windows)]
    [InlineData(220)]
    [InlineData(221)]
    public void PoolJournalLeftOnDiskIsGatheredAndReused(int journalDirLength)
    {
        string dataDir = NewDataDir(journalDirLength);
        string journalDir = Path.Combine(dataDir, "Journals");
        Directory.CreateDirectory(journalDir);
        string poolFile = Path.Combine(journalDir, StorageEnvironmentOptions.RecyclableJournalName(1));
        File.WriteAllBytes(poolFile, new byte[JournalSize]);
        File.SetCreationTimeUtc(poolFile, Sentinel);

        using var options = CreateOptions(dataDir, journalDirLength);
        using var env = new StorageEnvironment(options);
        Write(env, "k");

        Assert.Equal(Sentinel, File.GetCreationTimeUtc(GetJournals(options).Single()));
    }

    private string NewDataDir(int journalDirLength)
    {
        string root = NewDataPath(forceCreateDir: true);
        return Path.Combine(root, new string('p', journalDirLength - root.Length - @"\Journals".Length - 1));
    }

    private static StorageEnvironmentOptions CreateOptions(string dataDir, int journalDirLength)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(dataDir);
        Assert.Equal(journalDirLength, options.JournalPath.FullPath.Length);
        options.ManualFlushing = true;
        options.ManualSyncing = true;
        options.MaxLogFileSize = JournalSize;
        options.InitialLogFileSize = JournalSize;
        options.EnableJournalPoolPrewarming = false;
        return options;
    }

    private static string[] GetPoolFiles(StorageEnvironmentOptions options) =>
        Directory.GetFiles(options.JournalPath.FullPath, $"{StorageEnvironmentOptions.RecyclableJournalFileNamePrefix}.*");

    private static string[] GetJournals(StorageEnvironmentOptions options) =>
        Directory.GetFiles(options.JournalPath.FullPath, "*.journal");

    private static void Write(StorageEnvironment env, string key)
    {
        var value = new byte[24 * 1024];
        Random.Shared.NextBytes(value);
        using var tx = env.WriteTransaction();
        tx.CreateTree("t").Add(key, new MemoryStream(value));
        tx.Commit();
    }
}
