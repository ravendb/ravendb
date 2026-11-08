using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.Server.Config;
using Raven.Server.Utils;
using Sparrow.Platform;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Global;
using Voron.Impl.Paging;
using Xunit;
using NativeMemory = System.Runtime.InteropServices.NativeMemory;

namespace FastTests.Voron;

// The write ring and its file table are shared by the whole process: a test that limits the table or looks at its slots
// must not run next to tests that open data files
[CollectionDefinition(Name, DisableParallelization = true)]
public class IoRingFilesCollection
{
    public const string Name = nameof(IoRingFilesCollection);
}

[Collection(IoRingFilesCollection.Name)]
public unsafe class IoRingRegisteredFilesTests(ITestOutputHelper output) : NoDisposalNeeded(output)
{
    private const int FileSize = 64 * 1024;
    private const int PageSize = Constants.Storage.PageSize;

    [RavenFact(RavenTestCategory.Voron)]
    public void FullIoRingNamesTheConfigurationOptionThatControlsIt()
    {
        var file = Path.Combine("Databases", "Orders", Constants.DatabaseFilename);
        var error = Assert.Throws<InvalidOperationException>(() => Pager.RaiseError(file, errorCode: 24, PalFlags.FailCodes.FailIoRingFilesExhausted, 0));
        Output.WriteLine(error.Message);

        Assert.Contains(file, error.Message);
        Assert.Contains($"sets its size to {PalConfiguration.IoRingFileTableSize} files", error.Message);
        Assert.Contains("\"Storage.IoRingMaxFiles\":", error.Message);
        Assert.Contains("RAVEN_Storage_IoRingMaxFiles=", error.Message);
        Assert.Contains("--Storage.IoRingMaxFiles=", error.Message);
        Assert.Contains("restart the server", error.Message);
        Assert.Contains("'Storage.WriteMode'", error.Message);
        Assert.DoesNotContain("Too many open files", error.Message); // not the errno text of the PAL's EMFILE
    }

    [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Configuration)]
    public void IoRingMaxFilesDefaultsToTheProcessLimit()
    {
        var configured = PalConfiguration.IoRingMaxFiles;
        try
        {
            PalConfiguration.IoRingMaxFiles = null;
            Assert.Equal(PalConfiguration.FilesTheProcessCanOpen(), PalConfiguration.IoRingFileTableSize);
            PalConfiguration.IoRingMaxFiles = 16;
            Assert.Equal(16, PalConfiguration.IoRingFileTableSize);
            PalConfiguration.IoRingMaxFiles = int.MaxValue;
            Assert.Equal(PalConfiguration.FilesTheProcessCanOpen(), PalConfiguration.IoRingFileTableSize);
        }
        finally
        {
            PalConfiguration.IoRingMaxFiles = configured;
        }

        var configuration = RavenConfiguration.CreateForServer(null);
        configuration.Initialize();
        Assert.Null(configuration.Storage.IoRingMaxFiles);

        foreach (var value in new[] { "0", "-1" })
        {
            var invalid = RavenConfiguration.CreateForServer(null);
            invalid.SetSetting(RavenConfiguration.GetKey(x => x.Storage.IoRingMaxFiles), value);
            var error = Assert.ThrowsAny<Exception>(() => invalid.Initialize());
            Assert.Contains($"'Storage.IoRingMaxFiles' is set to {value}", error.Message);
        }
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void EachDataFileGetsItsOwnWrites()
    {
        if (IoRingIsUsed() == false)
            return;

        UsingDataDirectory(nameof(EachDataFileGetsItsOwnWrites), (options, path) =>
        {
            var files = Enumerable.Range(0, 8).Select(i => Open(options, Path.Combine(path, $"rf-{i}.voron"))).ToList();
            try
            {
                for (var i = 0; i < files.Count; i++)
                    WritePage(files[i], 1, (byte)(10 + i));
                for (var i = 0; i < files.Count; i++)
                    AssertPage(files[i].State, 1, (byte)(10 + i), $"file {i}, after every file was written");

                if (PlatformDetails.RunningOnLinux)
                {
                    var ring = WriteRingTable($"rf-0.voron");
                    for (var i = 0; i < files.Count; i++)
                        Assert.Single(ring.Files, f => f.Value == $"rf-{i}.voron");

                    var configured = PalConfiguration.IoRingFileTableSize;
                    Output.WriteLine($"the write ring holds {ring.Size} files, {configured} configured");
                    Assert.True(ring.Size == configured || (ring.Size == 1 << 15 && configured > 1 << 15),
                        $"the write ring holds {ring.Size} files, {configured} configured"); // older kernels hold at most 32768
                }
            }
            finally
            {
                files.ForEach(f => f.Dispose());
            }

            if (PlatformDetails.RunningOnLinux)
                Assert.DoesNotContain(RegisteredFiles().SelectMany(r => r.Files.Values), name => name.StartsWith("rf-"));
        });
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AClosedFileGetsNoWritesOfTheFileThatTakesItsSlot()
    {
        if (IoRingIsUsed() == false)
            return;

        UsingDataDirectory(nameof(AClosedFileGetsNoWritesOfTheFileThatTakesItsSlot), (options, path) =>
        {
            var closedPath = Path.Combine(path, "closed.voron");
            var closed = Open(options, closedPath);
            using var kept = Open(options, Path.Combine(path, "kept.voron"));
            WritePage(closed, 1, 11);
            WritePage(kept, 1, 12);
            var slot = PlatformDetails.RunningOnLinux ? WriteRingTable("closed.voron").SlotOf("closed.voron") : -1;
            closed.Dispose();

            using var next = Open(options, Path.Combine(path, "next.voron"));
            if (PlatformDetails.RunningOnLinux)
                Assert.Equal(slot, WriteRingTable("next.voron").SlotOf("next.voron")); // the lowest free slot

            WritePage(next, 1, 99);
            AssertPage(next.State, 1, 99, "the file in the reused slot");
            AssertPage(kept.State, 1, 12, "the file that kept its slot");

            using var handle = File.OpenHandle(closedPath);
            var page = new byte[PageSize];
            Assert.Equal(PageSize, RandomAccess.Read(handle, page, PageSize));
            Assert.True(page.All(b => b == 11), "the closed file was written after it was closed");
        });
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AGrownFileKeepsWritingThroughItsSlot()
    {
        if (IoRingIsUsed() == false)
            return;

        UsingDataDirectory(nameof(AGrownFileKeepsWritingThroughItsSlot), (options, path) =>
        {
            var file = Open(options, Path.Combine(path, "grown.voron"));
            try
            {
                WritePage(file, 1, 10);

                var before = file.State;
                const long pageAfterTheOldEnd = FileSize / PageSize + 4;
                file.Pager.EnsureContinuous(ref file.State, pageAfterTheOldEnd, 1);
                Assert.NotSame(before, file.State);
                before.Dispose(); // the slot outlives the handle that registered it

                WritePage(file, pageAfterTheOldEnd, 42);
                AssertPage(file.State, pageAfterTheOldEnd, 42, "after the file grew");
                AssertPage(file.State, 1, 10, "a page written before the file grew");
                file.Pager.Sync(file.State, 0);
            }
            finally
            {
                file.Dispose();
            }
        });
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void AFullTableFailsTheOpenAndFilesThatDoNotWriteThroughTheRingStillOpen()
    {
        if (IoRingIsUsed() == false)
            return;

        UsingDataDirectory(nameof(AFullTableFailsTheOpenAndFilesThatDoNotWriteThroughTheRingStillOpen), (options, path) =>
        {
            var readOnlyPath = Path.Combine(path, "read-only.voron");
            File.WriteAllBytes(readOnlyPath, new byte[FileSize]);

            var files = new List<DataFile>();
            try
            {
                using (LimitIoRingFiles(2))
                {
                    files.Add(Open(options, Path.Combine(path, "a.voron")));
                    files.Add(Open(options, Path.Combine(path, "b.voron")));

                    var error = Assert.Throws<InvalidOperationException>(() => Open(options, Path.Combine(path, "c.voron")));
                    Assert.Contains("Storage.IoRingMaxFiles", error.Message);

                    // read-only files and writable maps (scratch files) are not written through the ring
                    files.Add(Open(options, readOnlyPath, Pal.OpenFileFlags.ReadOnly));
                    files.Add(Open(options, Path.Combine(path, "scratch.voron"), Pal.OpenFileFlags.WritableMap | Pal.OpenFileFlags.Temporary));
                }

                WritePage(files[1], 1, 7);
                AssertPage(files[1].State, 1, 7, "a file opened before the table was full");
            }
            finally
            {
                files.ForEach(f => f.Dispose());
            }
        });
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void ClosedDataFilesReleaseTheirIoRingSlot()
    {
        if (IoRingIsUsed() == false)
            return;

        UsingDataDirectory(nameof(ClosedDataFilesReleaseTheirIoRingSlot), (options, path) =>
        {
            var file = Path.Combine(path, Constants.DatabaseFilename);
            using (LimitIoRingFiles(2))
            {
                // more open / close cycles than the ring has room for: a slot that is not released fails an open
                for (var i = 0; i < 64; i++)
                {
                    using var data = Open(options, file);
                    if (i % 8 != 0)
                        continue;
                    WritePage(data, 1, (byte)(i + 1));
                    AssertPage(data.State, 1, (byte)(i + 1), $"cycle {i}");
                }
            }
        });
    }

    [RavenFact(RavenTestCategory.Voron)]
    public void FilesOpenedAndClosedConcurrentlyGetOnlyTheirOwnWrites()
    {
        if (IoRingIsUsed() == false)
            return;

        const int threads = 8, rounds = 100, pages = 8;
        UsingDataDirectory(nameof(FilesOpenedAndClosedConcurrentlyGetOnlyTheirOwnWrites), (options, path) =>
        {
            var waitsOnAFullTable = 0;
            using (LimitIoRingFiles(4)) // fewer places than threads: the threads take each other's slots
            {
                Task.WaitAll(Enumerable.Range(0, threads).Select(thread => Task.Factory.StartNew(() =>
                {
                    var name = Path.Combine(path, $"churn-{thread}.voron");
                    byte last = 0;
                    for (var round = 1; round <= rounds; round++)
                    {
                        DataFile file;
                        while (TryOpen(options, name, out file) == false)
                        {
                            Interlocked.Increment(ref waitsOnAFullTable);
                            Thread.Yield();
                        }

                        using (file)
                        {
                            for (var page = 0; page < pages; page++)
                                AssertPage(file.State, page, last, $"thread {thread}, round {round}: the previous round's write");
                            last = (byte)(thread * 7 + round);
                            for (var page = 0; page < pages; page++) // the writes keep the file open while the other threads open theirs
                                WritePage(file, page, last);
                            for (var page = 0; page < pages; page++)
                                AssertPage(file.State, page, last, $"thread {thread}, round {round}");
                        }
                    }
                }, TaskCreationOptions.LongRunning)).ToArray()); // a thread each, whatever the thread pool has
            }

            Output.WriteLine($"{threads} threads x {rounds} rounds, {waitsOnAFullTable} waits on a full table");
            Assert.True(waitsOnAFullTable > 0, "the threads never found the table full");
        });
    }

    [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Windows)]
    public void ATemporaryFileIsDeletedWhenItIsClosed()
    {
        if (IoRingIsUsed() == false)
            return;

        UsingDataDirectory(nameof(ATemporaryFileIsDeletedWhenItIsClosed), (options, path) =>
        {
            var name = Path.Combine(path, "temporary.voron");
            using (var file = Open(options, name, Pal.OpenFileFlags.Temporary))
                WritePage(file, 0, 5);
            Assert.False(File.Exists(name), "the ring still holds the temporary file open");
        });
    }

    private bool IoRingIsUsed()
    {
        RuntimeHelpers.RunClassConstructor(typeof(Pal).TypeHandle); // resolves WriteMode=Auto
        Output.WriteLine($"write mode: {PalConfiguration.WriteMode}");
        return PalConfiguration.WriteMode == Pal.RvnWriteMode.IoRing; // not on macOS, nor with a kernel / container without io_uring
    }

    private static IDisposable LimitIoRingFiles(int max)
    {
        Assert.Equal(0, Pal.rvn_io_ring_limit_files_for_testing(max));
        return new Limit();
    }

    private sealed class Limit : IDisposable
    {
        public void Dispose() => Pal.rvn_io_ring_limit_files_for_testing(-1);
    }

    private static void UsingDataDirectory(string name, Action<StorageEnvironmentOptions, string> test)
    {
        var path = RavenTestHelper.NewDataPath(name, 0, forceCreateDir: true);
        try
        {
            using var options = StorageEnvironmentOptions.ForPathForTests(path);
            test(options, path);
        }
        finally
        {
            IOExtensions.DeleteDirectory(path);
        }
    }

    private sealed class DataFile(Pager pager, Pager.State state) : IDisposable
    {
        public readonly Pager Pager = pager;
        public Pager.State State = state;

        public void Dispose()
        {
            State.Dispose();
            Pager.Dispose();
        }
    }

    private static DataFile Open(StorageEnvironmentOptions options, string name, Pal.OpenFileFlags flags = Pal.OpenFileFlags.None)
    {
        var (pager, state) = Pager.Create(options, name, FileSize, flags);
        return new DataFile(pager, state);
    }

    private static bool TryOpen(StorageEnvironmentOptions options, string name, out DataFile file)
    {
        try
        {
            file = Open(options, name);
            return true;
        }
        catch (InvalidOperationException e) when (e.Message.Contains("Storage.IoRingMaxFiles"))
        {
            file = null;
            return false;
        }
    }

    private static void WritePage(DataFile file, long page, byte value)
    {
        var buffer = (byte*)NativeMemory.AlignedAlloc(PageSize, PageSize);
        try
        {
            new Span<byte>(buffer, PageSize).Fill(value);
            var write = new Pal.page_to_write { page_num = page, count_of_pages = 1, ptr = buffer };
            var rc = file.Pager.Write(file.State.Handle, &write, 1, out var errorCode);
            Assert.True(rc == PalFlags.FailCodes.Success, $"writing page {page} of {file.Pager.FileName} failed: {rc} ({errorCode})");
        }
        finally
        {
            NativeMemory.AlignedFree(buffer);
        }
    }

    private static void AssertPage(Pager.State state, long page, byte value, string what)
    {
        var data = new ReadOnlySpan<byte>(state.ReadAddress + page * PageSize, PageSize);
        var wrong = data.IndexOfAnyExcept(value);
        Assert.True(wrong == -1, $"{what}: page {page} holds {(wrong == -1 ? 0 : data[wrong])} at offset {wrong}, expected {value}");
    }

    private sealed record RingTable(int Size, Dictionary<int, string> Files)
    {
        public int SlotOf(string name) => Files.Single(f => f.Value == name).Key;
    }

    // the write ring is the ring that holds the file: data files opened without the prefetch flag are in no other ring
    private static RingTable WriteRingTable(string file)
    {
        for (var attempt = 0; attempt < 100; attempt++)
        {
            var ring = RegisteredFiles().SingleOrDefault(r => r.Files.ContainsValue(file));
            if (ring != null)
                return ring;
            Thread.Sleep(10); // the kernel skips the list while the ring is busy
        }
        throw new InvalidOperationException($"No io_uring of the process holds {file}");
    }

    // the file tables of the process's io_uring instances, from /proc/self/fdinfo: "UserFiles: <size>", then "<slot>: <name>" lines
    private static List<RingTable> RegisteredFiles()
    {
        var rings = new List<RingTable>();
        foreach (var fd in Directory.EnumerateFileSystemEntries("/proc/self/fd"))
        {
            string[] lines;
            try
            {
                if (new FileInfo(fd).LinkTarget != "anon_inode:[io_uring]")
                    continue;
                lines = File.ReadAllLines(Path.Combine("/proc/self/fdinfo", Path.GetFileName(fd)));
            }
            catch (IOException)
            {
                continue; // closed meanwhile
            }

            var size = -1;
            var files = new Dictionary<int, string>();
            foreach (var line in lines)
            {
                if (line.StartsWith("UserFiles:"))
                {
                    size = int.Parse(line["UserFiles:".Length..].Trim());
                    continue;
                }
                if (size == -1)
                    continue;
                var match = Regex.Match(line, @"^\s*(\d+):(.*)$");
                if (match.Success == false)
                    break;
                // some kernels list the empty slots too, and print only short names (an empty name for a long one)
                var name = match.Groups[2].Value.Trim();
                if (name.Length > 0 && name != "<none>")
                    files[int.Parse(match.Groups[1].Value)] = Path.GetFileName(name);
            }
            if (size != -1)
                rings.Add(new RingTable(size, files));
        }
        return rings;
    }
}
