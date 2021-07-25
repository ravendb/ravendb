using System.Runtime.CompilerServices;
using Sparrow.Platform;
using Sparrow.Server.Platform.Posix;
using Xunit.Abstractions;

namespace CoraxTests
{
    public abstract class LinuxRaceConditionWorkAround : XunitLoggingBase
    {
        static LinuxRaceConditionWorkAround()
        {
            XunitLogging.RedirectStreams = false;
            XunitLogging.Init();
            XunitLogging.EnableExceptionCapture();
            
            if (PlatformDetails.RunningOnPosix)
            {
                // open/close a file to force load assembly for parallel test success
                int fd = Syscall.open("/tmp/sqlReplicationPassword.txt", PerPlatformValues.OpenFlags.O_CREAT, FilePermissions.S_IRUSR);
                if (fd > 0)
                    Syscall.close(fd);
            }
        }

        protected LinuxRaceConditionWorkAround(ITestOutputHelper output, [CallerFilePath] string sourceFile = "") : base(output, sourceFile)
        {
        }
    }
}
