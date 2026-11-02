using System.IO;
using FastTests;
using Sparrow.Server.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Paging;
using Xunit;
using Constants = Voron.Global.Constants;

namespace SlowTests.Voron.Issues;

public class RavenDB_27665(ITestOutputHelper output) : RavenTestBase(output)
{
    // RavenDB-27665: when an io_uring write of the data file failed (cqe->res < 0), the PAL worker kept only the error flag and lost
    // the error code, so rvn_write_io_ring reported success, the flush took the pages as written and the sync retired the journal
    // that still held them. A pager opened read-only has an O_RDONLY descriptor, so every write through it fails; the PAL must say so.
    // The write mode is process-wide (RAVEN_Storage_WriteMode); IoRing and Auto exercise the fixed path.
    [RavenMultiplatformFact(RavenTestCategory.Voron, RavenPlatform.Linux)]
    public unsafe void FailedWriteToTheDataFileIsReportedAsAFailure()
    {
        var path = NewDataPath();
        Directory.CreateDirectory(path);
        var file = Path.Combine(path, Constants.DatabaseFilename);
        File.WriteAllBytes(file, new byte[4 * Constants.Storage.PageSize]);

        using var options = StorageEnvironmentOptions.ForPathForTests(path);
        var (pager, state) = Pager.Create(options, file, 0, Pal.OpenFileFlags.ReadOnly);
        using (pager)
        {
            var page = new byte[Constants.Storage.PageSize];
            fixed (byte* p = page)
            {
                var toWrite = new Pal.page_to_write { page_num = 1, count_of_pages = 1, ptr = p };
                var rc = pager.Write(state.Handle, &toWrite, 1, out var errno);
                Assert.True(rc != PalFlags.FailCodes.Success, $"a write to a read-only descriptor was reported as a success (errno {errno})");
            }
        }
    }
}
