using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Threading;
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
            WithoutReadAround(env);
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

    [RavenTheory(RavenTestCategory.Voron)]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    public void AColdPageIsReadInOneRequest(bool writeTransaction, bool headerFirst)
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        using (var options = CreateOptions(useEncryption: false))
        using (var env = new StorageEnvironment(options))
        {
            var pageNumber = WriteOnePage(env); // after the load: beyond the pages the environment validates
            WithoutReadAround(env);
            Evict(env);

            Assert.Equal(0, MajorFaultsReading(env, pageNumber, writeTransaction, headerFirst));
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AnEvictedPageIsHintedAgainOnceItsHintIsStale()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        using (var options = CreateOptions(useEncryption: false, policy))
        using (var env = new StorageEnvironment(options))
        {
            var pageNumber = WriteOnePage(env);
            WithoutReadAround(env);

            Evict(env);
            Assert.Equal(0, MajorFaultsReading(env, pageNumber));

            Evict(env);
            Assert.NotEqual(0, MajorFaultsReading(env, pageNumber)); // hinted already: each 4 KB is a fault of its own

            policy.AddEvictions(evictedFilePages: 1, filePages: 2); // the page cache turns over in 2 s: stale after 1 s
            policy.Tick(freeBytes: 0);
            Evict(env);
            Assert.Equal(0, MajorFaultsReading(env, pageNumber));
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ASequentialReadReadsAheadOfThePagesItReads()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        using (var options = CreateOptions(useEncryption: false))
        using (var env = new StorageEnvironment(options))
        {
            var first = WritePages(env, 64);
            WithoutReadAround(env);
            Evict(env);

            using (var tx = env.ReadTransaction())
            {
                var faults = MajorFaultsOfThisThread();
                for (var page = first; page < first + 4; page++)
                {
                    var read = tx.LowLevelTransaction.GetPage(page);
                    Assert.Equal(0x42, Volatile.Read(ref read.Pointer[Constants.Storage.PageSize - 1]));
                }
                Assert.Equal(0, MajorFaultsOfThisThread() - faults); // the first window is requested before its pages are read

                // the next window, past the pages read, comes in through the ring
                var deadline = DateTime.UtcNow.AddSeconds(10);
                while (ResidentPages(tx.LowLevelTransaction.DataPagerState, first + 6, 8) != 8 && DateTime.UtcNow < deadline)
                    Thread.Sleep(10);
                Assert.Equal(8, ResidentPages(tx.LowLevelTransaction.DataPagerState, first + 6, 8));
                Assert.Equal(0, ResidentPages(tx.LowLevelTransaction.DataPagerState, first + 32, 32));
            }
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ASequentialReadIntoALargeValueReadsAllOfIt()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        const int valuePages = 16;
        using (var options = CreateOptions(useEncryption: false))
        using (var env = new StorageEnvironment(options))
        {
            var first = WritePages(env, 3);
            long value;
            using (var tx = env.WriteTransaction())
            {
                var page = tx.LowLevelTransaction.AllocateOverflowRawPage(valuePages * Constants.Storage.PageSize - PageHeader.SizeOf, out _, zeroPage: true);
                value = page.PageNumber;
                Assert.Equal(first + 3, value); // inside the first window of the run
                tx.Commit();
            }
            env.FlushLogToDataFile();
            using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
                Assert.True(operation.SyncDataFile());
            WithoutReadAround(env);
            Evict(env);

            using (var tx = env.ReadTransaction())
            {
                var faults = MajorFaultsOfThisThread();
                for (var page = first; page < first + 3; page++)
                    _ = tx.LowLevelTransaction.GetPage(page);
                var read = tx.LowLevelTransaction.GetPage(value);
                for (var offset = 0; offset < valuePages * Constants.Storage.PageSize; offset += 4096)
                    _ = Volatile.Read(ref read.Pointer[offset]);
                Assert.Equal(0, MajorFaultsOfThisThread() - faults);
            }
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ReadAheadSkipsPagesHintedWithinTheStaleTime()
    {
        if (PlatformDetails.RunningOnLinux == false)
            return;

        var policy = new PrefetchPolicy(totalMemoryBytes: 0, startTimer: false);
        using (var options = CreateOptions(useEncryption: false, policy))
        {
            using (var env = new StorageEnvironment(options))
            {
                var first = WritePages(env, 40);
                using (var tx = env.ReadTransaction())
                {
                    var state = tx.LowLevelTransaction.DataPagerState;
                    Assert.Equal(8, Pager.ReadWindowAhead(state, first, 8, onCallingThread: true));
                    Assert.Equal(0, Pager.ReadWindowAhead(state, first, 8, onCallingThread: false)); // most likely still in memory

                    Pager.HintPage(state, first + 8);
                    Pager.HintPage(state, first + 9);
                    Assert.Equal(6, Pager.ReadWindowAhead(state, first + 8, 8, onCallingThread: true)); // from the first page not hinted

                    policy.AddEvictions(evictedFilePages: 1, filePages: 2); // stale after 1 s
                    policy.Tick(freeBytes: 0);
                    Assert.Equal(8, Pager.ReadWindowAhead(state, first, 8, onCallingThread: true));
                }
            }
        }
    }

    // a fault reads only the 4 KB it touches, so the major faults count what was not hinted
    private static void WithoutReadAround(StorageEnvironment env)
    {
        using var tx = env.ReadTransaction();
        var state = tx.LowLevelTransaction.DataPagerState;
        Assert.Equal(0, Syscall.madvise((IntPtr)state.ReadAddress, (UIntPtr)state.TotalAllocatedSize, MAdvFlags.MADV_RANDOM));
    }

    private static long WriteOnePage(StorageEnvironment env) => WritePages(env, 1);

    // adjacent single pages; returns the first
    private static long WritePages(StorageEnvironment env, int count)
    {
        long pageNumber = -1;
        using (var tx = env.WriteTransaction())
        {
            for (var i = 0; i < count; i++)
            {
                var page = tx.LowLevelTransaction.AllocatePage(1);
                new Span<byte>(page.DataPointer, Constants.Storage.PageSize - PageHeader.SizeOf).Fill(0x42);
                if (i == 0)
                    pageNumber = page.PageNumber;
                Assert.Equal(pageNumber + i, page.PageNumber);
            }
            tx.Commit();
        }

        env.FlushLogToDataFile();
        using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
            Assert.True(operation.SyncDataFile());
        return pageNumber;
    }

    // the major faults of reading the whole page
    private static long MajorFaultsReading(StorageEnvironment env, long pageNumber, bool writeTransaction = false, bool headerFirst = false)
    {
        using var tx = writeTransaction ? env.WriteTransaction() : env.ReadTransaction();
        var faults = MajorFaultsOfThisThread();
        if (headerFirst)
            Assert.Equal(pageNumber, tx.LowLevelTransaction.GetPageHeader<PageHeader>(pageNumber).PageNumber);
        var page = tx.LowLevelTransaction.GetPage(pageNumber);
        Assert.Equal(0x42, Volatile.Read(ref page.Pointer[Constants.Storage.PageSize - 1]));
        return MajorFaultsOfThisThread() - faults;
    }

    // pages whose both 4 KB halves are in the page cache
    private static int ResidentPages(Pager.State state, long firstPage, int count)
    {
        const int halves = Constants.Storage.PageSize / 4096;
        var vector = new byte[count * halves];
        fixed (byte* v = vector)
            Assert.Equal(0, mincore(state.ReadAddress + firstPage * Constants.Storage.PageSize, (UIntPtr)(count * Constants.Storage.PageSize), v));
        var resident = 0;
        for (var i = 0; i < count; i++)
        {
            if ((vector[i * halves] & vector[i * halves + 1] & 1) != 0)
                resident++;
        }
        return resident;
    }

    [DllImport("libc", SetLastError = true)]
    private static extern int mincore(void* addr, UIntPtr length, byte* vec);

    // drops the data file from this process' mappings, then from the page cache
    private void Evict(StorageEnvironment env)
    {
        using (var tx = env.ReadTransaction())
        {
            var state = tx.LowLevelTransaction.DataPagerState;
            Assert.Equal(0, Syscall.madvise((IntPtr)state.ReadAddress, (UIntPtr)state.TotalAllocatedSize, MAdvFlags.MADV_DONTNEED));
            if (state.WriteAddress != null)
                Assert.Equal(0, Syscall.madvise((IntPtr)state.WriteAddress, (UIntPtr)state.TotalAllocatedSize, MAdvFlags.MADV_DONTNEED));
        }
        DropFromPageCache(Path.Combine(DataDir, Constants.DatabaseFilename));
    }

    private StorageEnvironmentOptions CreateOptions(bool useEncryption, PrefetchPolicy policy = null)
    {
        var options = StorageEnvironmentOptions.ForPathForTests(DataDir);
        options.ManualFlushing = true;
        options.PrefetchPolicy = policy ?? MemoryIsFull; // no segment warm-up: it would read the page ahead on its own
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
