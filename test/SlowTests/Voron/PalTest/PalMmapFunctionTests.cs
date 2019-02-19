using System;
using System.IO;
using System.Runtime.InteropServices;
using FastTests;
using Voron.Platform;
using Xunit;

namespace SlowTests.Voron.PalTest
{
    public class PalMapTests : RavenTestBase
    {
        [Fact]
        public unsafe void MapFile_WhenCalled_ShouldSuccess()
        {
            //TODO To remove when mmap functions are implemented in windows
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == false)
                return;

            var path = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var initFileSize = 4096L;
            var mmapOptions = PalFlags.MmapOptions.CopyOnWrite;

            var ret = Pal.rvn_create_and_mmap64_file(path, initFileSize, mmapOptions, out var handle, out var baseAddress, out var actualFileSize, out var errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");
            
            ret = Pal.rvn_unmap(mmapOptions, baseAddress, actualFileSize, out errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");
            
            handle.Dispose();
        }

        [Fact]
        public unsafe void MapFileAndAllocateMoreSpace_WhenCalled_ShouldSuccess()
        {
            //TODO To remove when mmap functions are implemented in windows
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux) == false)
                return;

            var path = Path.Combine(NewDataPath(forceCreateDir: true), $"test_journal.{Guid.NewGuid()}");
            var initFileSize = 4096L;
            var mmapOptions = PalFlags.MmapOptions.CopyOnWrite;

            var ret = Pal.rvn_create_and_mmap64_file(path, initFileSize, mmapOptions, out var handle, out var baseAddress, out var actualFileSize, out var errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");

            ret = Pal.rvn_allocate_more_space(initFileSize, handle, out var newAddress, out errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");
            
            ret = Pal.rvn_unmap(mmapOptions, baseAddress, actualFileSize, out errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");
            
            ret = Pal.rvn_unmap(mmapOptions, newAddress, initFileSize, out errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");
            
            handle.Dispose();
        }
    }
}
