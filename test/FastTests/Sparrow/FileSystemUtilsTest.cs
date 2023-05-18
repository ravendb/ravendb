using Sparrow.Platform.Posix;
using Sparrow.Server.Platform.Posix;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Sparrow
{
    public class FileSystemUtilsTest : RavenTestBase
    {
        public FileSystemUtilsTest(ITestOutputHelper output) : base(output)
        {
        }

        [LinuxFact]
        public void WriteAndReadPageUsingCryptoPager_WhenCall_AllResultShouldBeNotNull()
        {
            var swap = KernelVirtualFileSystemUtils.ReadSwapInformationFromSwapsFile();
            Assert.All(swap, s => Assert.NotNull(s.DeviceName));
        }
        
        [LinuxFact]
        public void WriteAndReadPageUsingCryptoPager_WhenTwoSwapDefined_AllResultShouldBeNotNull()
        {
            var fileName = GetTempFileName();
            
            string[] lines =
            {
                "Filename				Type		Size	Used	Priority", 
                "/dev/sda6                               partition	999420	56320	-2", 
                "/dev/sdb7                               partition	999420	56320	-2"
            };
            System.IO.File.WriteAllLines(fileName, lines);
            
            var swap = KernelVirtualFileSystemUtils.ReadSwapInformationFromSwapsFile(fileName);
            Assert.All(swap, s => Assert.NotNull(s.DeviceName));
        }
        
        [LinuxFact]
        public void CGroup_WhenTryingToGetData_SuccessToGetValue()
        {
            var cgroup = CGroupHelper.CGroup;
            Assert.NotNull(cgroup.GetMaxMemoryUsage());
            Assert.NotNull(cgroup.GetPhysicalMemoryLimit());
            Assert.NotNull(cgroup.GetPhysicalMemoryUsage());
        }
    }
}
