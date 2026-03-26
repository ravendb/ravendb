using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Xunit;

namespace FastTests
{
    public abstract class LinuxRaceConditionWorkAround : IAsyncDisposable
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

        protected LinuxRaceConditionWorkAround(ITestOutputHelper output, [CallerFilePath] string sourceFile = "")
        {
            Output = output;
        }

        protected ITestOutputHelper Output { get; }

        public virtual ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }
}
