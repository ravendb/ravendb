using System;
using System.IO;
using System.Linq;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Xunit;

namespace FastTests.Voron;

public unsafe class ColdPageReadTests(ITestOutputHelper output) : StorageTest(output)
{
    private const int PosixFadviseDontNeed = 4;

    private static readonly PrefetchPolicy MemoryIsFull = new(totalMemoryBytes: 0, startTimer: false);

    private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

    // data files are opened with random-access advice: without read-ahead, each 4 KB of a cold page is a major fault of its own
    [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
    [InlineData(false)]
    [InlineData(true)]
    public void AColdPageIsReadAheadBeforeItIsCopied(bool useEncryption)
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        const int pages = 8;
        long pageNumber;
        using (var options = CreateOptions(useEncryption))
        using (var env = new StorageEnvironment(options))
        {
            using (var tx = env.WriteTransaction())
            {
                var page = tx.LowLevelTransaction.AllocateOverflowRawPage(pages * Constants.Storage.PageSize - PageHeader.SizeOf, out _, zeroPage: true);
                pageNumber = page.PageNumber;
                tx.Commit();
            }

            env.FlushLogToDataFile();
            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
                Assert.True(operation.SyncDataFile());
        }

        using (var options = CreateOptions(useEncryption))
        using (var env = new StorageEnvironment(options))
        {
            DropFromPageCache(Path.Combine(DataDir, Constants.DatabaseFilename));
            using (var tx = env.ReadTransaction())
            {
                var faults = MajorFaultsOfThisThread();
                tx.LowLevelTransaction.GetPage(pageNumber);
                faults = MajorFaultsOfThisThread() - faults;

                Output.WriteLine($"major faults reading a cold {pages} pages value: {faults}");
                Assert.True(faults <= 1, $"{faults} major faults for {pages} pages"); // read ahead: the copy waits for reads already started
            }
        }
    }

    private StorageEnvironmentOptions CreateOptions(bool useEncryption)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(DataDir);
        options.ManualFlushing = true;
        options.PrefetchPolicy = MemoryIsFull; // no segment warm-up: it would read the page ahead on its own
        if (useEncryption)
            options.Encryption.MasterKey = _masterKey.ToArray();
        return options;
    }

    private static void DropFromPageCache(string file)
    {
        using var stream = new FileStream(file, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        var fd = stream.SafeFileHandle.DangerousGetHandle().ToInt32();
        Assert.Equal(0, Syscall.posix_fadvise(fd, 0, 0, PosixFadviseDontNeed));
    }

    // majflt, the 12th field of /proc/thread-self/stat; the fields after the command name start at the 3rd
    private static long MajorFaultsOfThisThread()
    {
        var stat = File.ReadAllText("/proc/thread-self/stat");
        var fields = stat[(stat.LastIndexOf(')') + 2)..].Split(' ');
        return long.Parse(fields[12 - 3]);
    }
}
