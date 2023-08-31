using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
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
            if (cgroup.ForTestIsControllerMemoryGroupsAvailable() == false)
                return;
            
            //cgroup2 does not always have memory.peak
            try
            {
                cgroup.GetMaxMemoryUsage();
                Assert.True(cgroup.GetPhysicalMemoryLimit() != null, $"Failed to {nameof(CGroup.GetPhysicalMemoryLimit)}");
                Assert.True(cgroup.GetPhysicalMemoryUsage() != null, $"Failed to {nameof(CGroup.GetPhysicalMemoryUsage)}");
            }
            catch (Exception e)
            {
                var message = new StringBuilder($"Failed to get memory values{Environment.NewLine}");
                var exceptions = new List<Exception> {e};
                foreach (var file in new []{CGroup.PROC_CGROUPS_FILENAME, CGroup.PROC_MOUNTINFO_FILENAME, CGroup.PROC_SELF_CGROUP_FILENAME})
                {
                    if (TryRead(file, out string content, exceptions) == false) 
                        continue;
                    
                    message.AppendLine($"File name {file}");
                    message.AppendLine(content);
                }

                try
                {
                    var path = cgroup.ForTestFindCGroupPathForMemory();
                    if (path != null)
                    {
                        message.AppendLine($"{path} cgroup folder content");
                        message.AppendLine(string.Join(Environment.NewLine, Directory.GetFiles(path)));
                    }
                }
                catch (Exception exception)
                {
                    exceptions.Add(exception);
                }
                
                throw new AggregateException(message.ToString(), exceptions);
                
                bool TryRead(string fileName, out string content, List<Exception> exceptions)
                {
                    try
                    {
                        content = File.ReadAllText(fileName);
                        return true;
                    }
                    catch(Exception exception)
                    {
                        exceptions.Add(exception);
                        content = null;
                        return false;
                    }
                }
            }
        }
    }
}
