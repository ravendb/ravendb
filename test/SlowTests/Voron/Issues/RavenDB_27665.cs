using System;
using System.IO;
using FastTests;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Posix;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Paging;
using Xunit;
using Constants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues;

public class RavenDB_27665(ITestOutputHelper output) : RavenTestBase(output)
{
    private const int EFAULT = 14;

    // RavenDB-27665: when an io_uring write of the data file failed (cqe->res < 0), the PAL worker kept only the error flag and lost
    // the error code, so rvn_write_io_ring reported success, the flush took the pages as written and the sync retired the journal
    // that still held them. The source page is a PROT_NONE mapping: the PAL cannot see that, so the write reaches the ring and the
    // kernel fails it with -EFAULT. The PAL must report the failure with its code.
    // The write mode is process-wide (RAVEN_Storage_WriteMode); IoRing and Auto exercise the fixed path.
    [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Linux)]
    public unsafe void FailedWriteToTheDataFileIsReportedAsAFailure()
    {
        var path = NewDataPath();
        Directory.CreateDirectory(path);
        var file = Path.Combine(path, Constants.DatabaseFilename);
        File.WriteAllBytes(file, new byte[4 * Constants.Storage.PageSize]);

        var size = (UIntPtr)Constants.Storage.PageSize;
        var unreadable = Syscall.mmap64(IntPtr.Zero, size, MmapProts.PROT_NONE,
            MmapFlags.MAP_PRIVATE | PerPlatformValues.MmapFlags.MAP_ANONYMOUS, -1, 0);
        Assert.NotEqual(new IntPtr(-1), unreadable);
        try
        {
            using var options = StorageEnvironmentOptions.ForPathForTests(path);
            var (pager, state) = Pager.Create(options, file, 0, Pal.OpenFileFlags.None);
            using (pager)
            {
                var toWrite = new Pal.page_to_write { page_num = 1, count_of_pages = 1, ptr = (void*)unreadable };
                var rc = pager.Write(state.Handle, &toWrite, 1, out var errno);
                Assert.True(rc != PalFlags.FailCodes.Success, $"a write the kernel failed was reported as a success (errno {errno})");
                Assert.Equal(EFAULT, errno);
            }
        }
        finally
        {
            Syscall.munmap(unreadable, size);
        }
    }
}
