using System;
using System.IO;
using System.Runtime.InteropServices;
using FastTests;
using Sparrow.Server.Platform;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.PalTest
{
    public class PalMapTests : RavenTestBase
    {
        public PalMapTests(ITestOutputHelper output) : base(output)
        {
        }

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

            ret = Pal.rvn_allocate_more_space(mapAfterAllocationFlag: 1, initFileSize, handle, out var newAddress, out errorCode);
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
        
        [FactForLinuxOnly]
        public unsafe void MMap_WhenLinkEndOfPath_ShouldSucceed()
        {
            var basicPath = NewDataPath(forceCreateDir: true);
            
            var linkTarget = Path.Combine(basicPath, "linkTarget");
            Directory.CreateDirectory(linkTarget);
            
            var link = Path.Combine(basicPath, "l");
            symlink(linkTarget, link);

            var filePath = Path.Combine(link, $"test_journal.{Guid.NewGuid()}");
            var initFileSize = 4096L;
            var mmapOptions = PalFlags.MmapOptions.CopyOnWrite;

            var ret = Pal.rvn_create_and_mmap64_file(filePath, initFileSize, mmapOptions, out var handle, out var baseAddress, out var actualFileSize, out var errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");
            
            ret = Pal.rvn_unmap(mmapOptions, baseAddress, actualFileSize, out errorCode);
            if (ret != PalFlags.FailCodes.Success)
                PalHelper.ThrowLastError(ret, errorCode, "");
            
            handle.Dispose();
        }
        
        [FactForLinuxOnly]
        public unsafe void MMap_WhenLinkEndOfExistPath_ShouldSucceed()
        {
            var basicPath = NewDataPath(forceCreateDir: true);
            
            var linkTarget = Path.Combine(basicPath, "linkTarget");
            Directory.CreateDirectory(linkTarget);
            
            var link = Path.Combine(basicPath, "l");
            symlink(linkTarget, link);

            var filePath = Path.Combine(link, "a", "b", $"test_journal.{Guid.NewGuid()}");
            var initFileSize = 4096L;
            var mmapOptions = PalFlags.MmapOptions.CopyOnWrite;

            var ret = Pal.rvn_create_and_mmap64_file(filePath, initFileSize, mmapOptions, out var handle, out var baseAddress, out var actualFileSize, out var errorCode);
            using (handle)
            {
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errorCode, "");
            
                ret = Pal.rvn_unmap(mmapOptions, baseAddress, actualFileSize, out errorCode);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errorCode, "");
            }
        }
        
        [FactForLinuxOnly]
        public unsafe void MMap_WhenLinkTargetContainsPartOfThePath_ShouldSucceed()
        {
            var basicPath = NewDataPath(forceCreateDir: true);
            
            var linkTarget = Path.Combine(basicPath, "linkTarget");
            Directory.CreateDirectory(linkTarget);
            
            var link = Path.Combine(basicPath, "l");
            symlink(linkTarget, link);

            Directory.CreateDirectory(linkTarget);
            var linkPlus = Path.Combine(link, "a", "b", $"test_journal.{Guid.NewGuid()}");
            var filePath = Path.Combine(linkPlus, "b", $"test_journal.{Guid.NewGuid()}");
            var initFileSize = 4096L;
            var mmapOptions = PalFlags.MmapOptions.CopyOnWrite;

            var ret = Pal.rvn_create_and_mmap64_file(filePath, initFileSize, mmapOptions, out var handle, out var baseAddress, out var actualFileSize, out var errorCode);
            using (handle)
            {
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errorCode, "");
            
                ret = Pal.rvn_unmap(mmapOptions, baseAddress, actualFileSize, out errorCode);
                if (ret != PalFlags.FailCodes.Success)
                    PalHelper.ThrowLastError(ret, errorCode, "");
            }
        }
        
        [FactForLinuxOnly]
        public unsafe void MMap_WhenLinkBroken_ShouldFailWithInfoError()
        {
            var basicPath = NewDataPath(forceCreateDir: true);
            
            var linkTarget = Path.Combine(basicPath, "brokentarget");
            
            var link = Path.Combine(basicPath, "brokenlink");
            symlink(linkTarget, link);

            var filePath = Path.Combine(link, $"test_journal.{Guid.NewGuid()}");
            var initFileSize = 4096L;
            var mmapOptions = PalFlags.MmapOptions.CopyOnWrite;

            var ret = PalFlags.FailCodes.None;
            Exception ex = Assert.Throws<InvalidOperationException>(() =>
            {
                ret = Pal.rvn_create_and_mmap64_file(filePath, initFileSize, mmapOptions, out var handle, out var baseAddress, out var actualFileSize, out var errorCode);
                using (handle)
                {
                    if (ret != PalFlags.FailCodes.Success)
                        PalHelper.ThrowLastError(ret, errorCode, "");
            
                    ret = Pal.rvn_unmap(mmapOptions, baseAddress, actualFileSize, out errorCode);
                    if (ret != PalFlags.FailCodes.Success)
                        PalHelper.ThrowLastError(ret, errorCode, "");
                }
            });
                 
            Assert.Equal(PalFlags.FailCodes.FailBrokenLink, ret);
        }
        
        private const string LIBC_6 = "libc";

        [DllImport(LIBC_6, SetLastError = true)]
        static extern bool symlink(string linkContent, string link);
    }
}

public class FactForLinuxOnlyAttribute : FactAttribute 
{
    public FactForLinuxOnlyAttribute() {
        if(RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
        {
            Skip = "For Posix only";
        }
    }
}
