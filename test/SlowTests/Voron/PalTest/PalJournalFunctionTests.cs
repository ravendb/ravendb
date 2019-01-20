using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using FastTests;
using Sparrow.Utils;
using Voron.Impl.Journal;
using Voron.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.PalTest
{
    public class PalJournalTests : RavenTestBase
    {
        [Fact]
        public unsafe void rvn_get_error_string_WhenCalled_ShouldCreateFile()
        {
            var errBuffer = new byte[256];

            fixed (byte* p = errBuffer)
            {
                var size = Pal.rvn_get_error_string(
                    0,
                    p,
                    256,
                    out var errno
                );

                var errorString = Encoding.UTF8.GetString(p, size);

                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    Assert.StartsWith("The operation completed successfully.", errorString);
                else if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    Assert.StartsWith("Success", errorString);
            }
        }

        [Fact]
        public void OpenJournal_WhenCalled_ShouldCreateFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var fileSize = 4096L;
            PalFlags.FailCodes ret;
            
            ret = Pal.rvn_open_journal_for_writes(file, (int)PalFlags.JournalMode.Safe, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            Assert.Equal(PalFlags.FailCodes.Success, ret);

            Assert.True(File.Exists(file));
            Assert.False(handle.IsInvalid);

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var length = new FileInfo(file).Length;
            Assert.True(length >= 4096L);
            Assert.Equal(length, actualSize);
        }

        [Fact]
        public unsafe void WriteJournal_WhenCalled_ShouldWriteOnFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var fileSize = 4096L;
            PalFlags.FailCodes ret;

            ret = Pal.rvn_open_journal_for_writes(file, (int)PalFlags.JournalMode.Safe, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[4096];
            Marshal.Copy((IntPtr)buffer, expected, 0, 4096);
            try
            {
                ret = Pal.rvn_write_journal(handle, buffer, 4096, 0, out errno);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var bytesFromFile = File.ReadAllBytes(file);

            Assert.Equal(expected, bytesFromFile);
        }

        [Fact]
        public unsafe void WriteJournal_WhenOffsetNotOnFileBeginning_ShouldWriteOnFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var fileSize = 3 * 4096L;
            PalFlags.FailCodes ret;

            ret = Pal.rvn_open_journal_for_writes(file, (int)PalFlags.JournalMode.Safe, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[fileSize];
            Marshal.Copy((IntPtr)buffer, expected, 4096, 4096);
            try
            {
                ret = Pal.rvn_write_journal(handle, buffer, 4096, 4096, out errno);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var bytesFromFile = File.ReadAllBytes(file);

            Assert.Equal(expected, bytesFromFile);
        }

        [Fact]
        public unsafe void ReadJournal_WhenDo_ShouldRead()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            const int fileSize = 3 * 4096;
            const int offset = 4096;
            const int length = 4096;
            PalFlags.FailCodes ret;
            
            ret = Pal.rvn_open_journal_for_writes(file, (int)PalFlags.JournalMode.Safe, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[fileSize];
            Marshal.Copy((IntPtr)buffer, expected, offset, length);
            try
            {
                ret = Pal.rvn_write_journal(handle, buffer, length, offset, out errno);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var actual = new byte[length];
            fixed (byte* pActual = actual)
            {
                ret = Pal.rvn_open_journal_for_reads(file, out var rHandle, out errno);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errno, "");
                
                ret = Pal.rvn_read_journal(rHandle, pActual, length, offset, out var readActualSize, out errno);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errno, "");

                Assert.Equal(length, readActualSize);
            }

            Assert.Equal(expected, expected);
        }

        [Fact]
        public unsafe void ReadJournal_WhenPassingTheEnd_ShouldReadUntilTheEndAndReturnEOF()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            const int fileSize = 4096;
            const int offset = fileSize - 500;
            const int readLength = 1000;
            PalFlags.FailCodes ret;

            ret = Pal.rvn_open_journal_for_writes(file, PalFlags.JournalMode.Safe, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[500];
            Marshal.Copy((IntPtr)buffer + offset, expected, 0, 500);
            try
            {
                ret = Pal.rvn_write_journal(handle, buffer, fileSize, 0, out errno);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var actual = new byte[readLength];
            fixed (byte* pActual = actual)
            {
                ret = Pal.rvn_open_journal_for_reads(file, out var rHandle, out errno);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errno, "");
                
                ret = Pal.rvn_read_journal(rHandle, pActual, 1000, offset, out var readActualSize, out errno);
                
                if (ret != PalFlags.FailCodes.FailEndOfFile)
                    PalHelper.ThrowLastError(ret, errno, "");
                
                Assert.Equal(500, readActualSize);
            }

            Assert.Equal(expected, actual.Take(500));
        }

        [Fact]
        public void TruncateJournal_WhenCalled_ShouldTruncate()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            PalFlags.FailCodes ret;
            
            const long initSize = 2 * 4096L;
            ret = Pal.rvn_open_journal_for_writes(file, (int)PalFlags.JournalMode.Safe, initSize, out var handle, out var actualSize, out var errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            const long truncateSize = 4096L;
            ret = Pal.rvn_truncate_journal(file, handle, truncateSize, out errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");

            var actual = new FileInfo(file).Length;
            Assert.Equal(truncateSize, actual);

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errno, "");
        }
    }
}
