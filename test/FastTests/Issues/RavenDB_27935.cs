using System.IO;
using System.Runtime.InteropServices;
using Sparrow.Server.Platform;
using Sparrow.Server.Platform.Posix;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public unsafe class RavenDB_27935 : NoDisposalNeeded
    {
        public RavenDB_27935(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformFact(RavenTestCategory.Pal, RavenPlatform.Linux | RavenPlatform.OsX)]
        public void AFailedCreateMustNotCloseFileDescriptorZero()
        {
            int stdin = dup(0);
            if (stdin == -1)
                Assert.Skip("fd 0 is not open in this process");

            // the parent "directory" is a regular file, so the create fails before it opens the file
            var parent = Path.GetTempFileName();
            try
            {
                var rc = Pal.rvn_create_and_mmap64_file(Path.Combine(parent, "sub", "file.voron"), 64 * 1024, PalFlags.MmapOptions.None,
                    out var handle, out _, out _, out _);

                // dup takes the lowest free descriptor, so it gets 0 (and puts fd 0 back) only if the create closed fd 0.
                // Check at once: the next file the process opens takes a free fd 0.
                int lowest = dup(stdin);
                handle.Dispose();
                if (lowest > 0)
                    Syscall.close(lowest);

                Assert.NotEqual(PalFlags.FailCodes.Success, rc);
                Assert.True(lowest > 0, $"the failed create closed fd 0 (dup returned {lowest})");
            }
            finally
            {
                Syscall.close(stdin);
                File.Delete(parent);
            }
        }

        [DllImport("libc", SetLastError = true)]
        private static extern int dup(int fd);
    }
}
