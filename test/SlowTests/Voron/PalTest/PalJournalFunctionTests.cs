using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using System.Text;
using FastTests;
using Sparrow.Utils;
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
            var ret = 0;
            
            ret = Pal.rvn_open_journal(file, (int)PalFlags.JOURNAL_MODE.SAFE, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            Assert.Equal((int)PalFlags.FailCodes.Success, ret);

            Assert.True(File.Exists(file));
            Assert.NotEqual(IntPtr.Zero, handle);

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var length = new FileInfo(file).Length;
            Assert.True(length >= 4096L);
            Assert.Equal(length, actualSize);
        }

        [Fact]
        public void OpenJournal_WhenSizeRequiredIsNotAlignTo4K_TheActualSizeShouldBeGreaterAndAlignTo4K()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var fileSize = 4096L + 1000;
            var ret = 0;
            
            Pal.rvn_open_journal(file, (int)PalFlags.JOURNAL_MODE.SAFE, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            Assert.Equal((int)PalFlags.FailCodes.Success, ret);

            Assert.True(File.Exists(file));
            Assert.NotEqual(IntPtr.Zero, handle);

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var length = new FileInfo(file).Length;
            Assert.True(length % 4096L == 0);
            Assert.Equal(length, actualSize);
        }

        [Fact]
        public unsafe void WriteJournal_WhenCalled_ShouldWriteOnFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var fileSize = 4096L;
            var ret = 0;

            ret = Pal.rvn_open_journal(file, (int)PalFlags.JOURNAL_MODE.SAFE, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[4096];
            Marshal.Copy((IntPtr)buffer, expected, 0, 4096);
            try
            {
                ret = Pal.rvn_write_journal(handle, (IntPtr)buffer, 4096, 0, out errno);
                if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                    PalHelper.ThrowLastError(errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var bytesFromFile = File.ReadAllBytes(file);

            Assert.Equal(expected, bytesFromFile);
        }

        [Fact]
        public unsafe void WriteJournal_WhenOffsetNotOnFileBeginning_ShouldWriteOnFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var fileSize = 3 * 4096L;
            var ret = 0;

            ret = Pal.rvn_open_journal(file, (int)PalFlags.JOURNAL_MODE.SAFE, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[fileSize];
            Marshal.Copy((IntPtr)buffer, expected, 4096, 4096);
            try
            {
                ret = Pal.rvn_write_journal(handle, (IntPtr)buffer, 4096, 4096, out errno);
                if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                    PalHelper.ThrowLastError(errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

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
            var ret = 0;
            ret = Pal.rvn_open_journal(file, (int)PalFlags.JOURNAL_MODE.SAFE, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[fileSize];
            Marshal.Copy((IntPtr)buffer, expected, offset, length);
            try
            {
                ret = Pal.rvn_write_journal(handle, (IntPtr)buffer, (ulong)length, offset, out errno);
                if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                    PalHelper.ThrowLastError(errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var actual = new byte[length];
            fixed (byte* p = actual)
            {
                var rHandle = new IntPtr(-1);
                ret = Pal.rvn_read_journal(file, ref rHandle, p, (ulong)length, offset, out var readActualSize, out errno);
                if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                    PalHelper.ThrowLastError(errno, "");

                Assert.Equal((ulong)length, readActualSize);
            }

            Assert.Equal(expected, expected);
        }

        [Fact]
        public unsafe void ReadJournal_WhenPassingTheEnd_ShouldReadUntilTheEnd()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            const int fileSize = 4096;
            const int offset = fileSize - 500;
            const int readLength = 1000;
            var ret = 0;

            ret = Pal.rvn_open_journal(file, (int)PalFlags.JOURNAL_MODE.SAFE, fileSize, out var handle, out var actualSize, out var errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var buffer = NativeMemory.Allocate4KbAlignedMemory(4096, out var stats);
            for (var i = 0; i < 4096 / sizeof(int); i++)
            {
                *((int*)buffer + i) = i;
            }
            var expected = new byte[500];
            Marshal.Copy((IntPtr)buffer + offset, expected, 0, 500);
            try
            {
                ret = Pal.rvn_write_journal(handle, (IntPtr)buffer, (ulong)fileSize, 0, out errno);
                if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                    PalHelper.ThrowLastError(errno, "");
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var actual = new byte[readLength];
            fixed (byte* p = actual)
            {
                var rHandle = new IntPtr(-1);
                ret = Pal.rvn_read_journal(file, ref rHandle, p, (ulong)1000, offset, out var readActualSize, out errno);
                if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                    PalHelper.ThrowLastError(errno, "");

                Assert.Equal((ulong)500, readActualSize);
            }

            Assert.Equal(expected, actual.Take(500));
        }

        [Fact]
        public void TruncateJournal_WhenCalled_ShouldTruncate()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var ret = 0;
            
            const long initSize = 2 * 4096L;
            ret = Pal.rvn_open_journal(file, (int)PalFlags.JOURNAL_MODE.SAFE, initSize, out var handle, out var actualSize, out var errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            const long truncateSize = 4096L;
            ret = Pal.rvn_truncate_journal(file, handle, truncateSize, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");

            var actual = new FileInfo(file).Length;
            Assert.Equal(truncateSize, actual);

            ret = Pal.rvn_close_journal(handle, out errno);
            if (ret != (int)PalFlags.FAIL_CODES.SUCCESS)
                PalHelper.ThrowLastError(errno, "");
        }
    }
}
