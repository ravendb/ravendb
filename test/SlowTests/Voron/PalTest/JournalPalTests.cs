using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using FastTests;
using Sparrow.Utils;
using Voron.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.PalTest
{
    public class JournalPalTests : RavenTestBase
    {
        private readonly ITestOutputHelper _testOutputHelper;

        public JournalPalTests(ITestOutputHelper testOutputHelper)
        {
            _testOutputHelper = testOutputHelper;
        }

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
                    out int errno
                );
                
                
                _testOutputHelper.WriteLine(""+size);
                var errorString = Encoding.UTF8.GetString(p, size);

                if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
                    Assert.Equal("The operation completed successfully.\r", errorString);
                else if(RuntimeInformation.IsOSPlatform(OSPlatform.Linux))
                    Assert.Equal("Success", errorString);
            }
        }

        [Fact]
        public void OpenJournal_WhenCalled_ShouldCreateFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var ret = Pal.open_journal(file, 0, 4096, out IntPtr handle, out uint errno);
            if(errno != 0)
                PalHelper.ThrowLastError(errno, "");
            
            Assert.Equal(0, ret);
            
            Assert.True(File.Exists(file));
            Assert.NotEqual(IntPtr.Zero, handle);

            Pal.close_journal(handle, out errno);
            if (errno != 0)
                PalHelper.ThrowLastError(errno, "");
        }

        [Fact]
        public unsafe void WriteJournal_WhenCalled_ShouldCreateFile()
        {
            var file = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            if (Pal.open_journal(file, 0, 4096, out IntPtr handle, out uint errno) != 0)
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
                if (Pal.write_journal(handle, (IntPtr)buffer, 4096, 0, out errno) != 0)
                    PalHelper.ThrowLastError(errno, ""); 
            }
            finally
            {
                NativeMemory.Free4KbAlignedMemory(buffer, 4096, stats);
            }

            if (Pal.close_journal(handle, out errno) != 0)
                PalHelper.ThrowLastError(errno, "");

            var bytesFromFile = File.ReadAllBytes(file);
            
            Assert.Equal(expected, bytesFromFile);
        }
    }
}
