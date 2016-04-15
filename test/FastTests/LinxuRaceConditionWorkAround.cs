using Microsoft.CodeAnalysis;
using Voron.Platform.Posix;
using Platform = Sparrow.Platform.Platform;

namespace FastTests
{
    public class LinuxRaceConditionWorkAround
    {
        static LinuxRaceConditionWorkAround()
        {
            if (Platform.RunningOnPosix)
            {
                // open/close a file to force load assembly for parallel test success
                int fd = Syscall.open("/tmp/sqlReplicationPassword.txt", OpenFlags.O_CREAT, FilePermissions.S_IRUSR);
                if (fd > 0)
                    Syscall.close(fd);
            }
        }

    }
}