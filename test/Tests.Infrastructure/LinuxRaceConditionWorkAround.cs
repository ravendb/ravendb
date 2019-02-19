using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;

namespace FastTests
{
    public class LinuxRaceConditionWorkAround
    {
        static LinuxRaceConditionWorkAround()
        {
            if (PlatformDetails.RunningOnPosix)
            {
                // open/close a file to force load assembly for parallel test success
                int fd = Syscall.open("/tmp/sqlReplicationPassword.txt", PerPlatformValues.OpenFlags.O_CREAT, FilePermissions.S_IRUSR);
                if (fd > 0)
                    Syscall.close(fd);
            }
        }

    }
}
