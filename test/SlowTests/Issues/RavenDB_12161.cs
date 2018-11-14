using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using FastTests;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Logging;
using Xunit;
using Assert = Xunit.Assert;

namespace SlowTests.Issues
{
    public class RavenDB_12161 : RavenTestBase
    {
        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessRun_ShouldGetValue()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            string exec;
            string args;
            var jsonCpuUsage = "{\"MachineCpuUsage\":57, \"ProcessCpuUsage\":2.5}";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                jsonCpuUsage = $"\"{jsonCpuUsage.Replace("\"", "`\"")}\"";

                var pshScript = string.Format(@"
while($TRUE){{
	Write-Host {0}
	Start-Sleep 1
}}", jsonCpuUsage);

                args = "-NoProfile " + tempFileName;
                File.WriteAllText(tempFileName, pshScript);
            }
            else 
            {
                var bashScript = "#!/bin/bash \nfor i in {1..100} \ndo \n 	echo " + jsonCpuUsage.Replace("\"", "\\\"") + " \n	sleep 1 \ndone";

                exec = "bash";
                args = tempFileName;
                File.WriteAllText(tempFileName, bashScript);
                Process.Start("chmod", $"755 {tempFileName}");
            }
            
            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                args,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointData { ProcessCpuUsage = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 10)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.Equal(57, value.MachineCpuUsage);
                Assert.Equal(2.5, value.ProcessCpuUsage);
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessExit_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            string exec;
            string args;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                const string pshScript = "";

                args = "-NoProfile " + tempFileName;
                File.WriteAllText(tempFileName, pshScript);
            }
            else 
            {
                const string bashScript = "#!/bin/bash";

                exec = "bash";
                args = tempFileName;
                File.WriteAllText(tempFileName, bashScript);
                Process.Start("chmod", $"755 {tempFileName}");
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                args,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointData{ ProcessCpuUsage = 0, MachineCpuUsage = 0};
                while (Math.Abs(value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 10)
                    {
                        throw new TimeoutException();
                    }
                     value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ProcessCpuUsage < 0, $"Got {value} {nameof(value.ProcessCpuUsage)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process exited");
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessReturnInvalidData_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            string exec;
            string args;
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";

                const string pshScript = @"
while($TRUE){{
	Write-Host David
	Start-Sleep 1
}}";

                args = "-NoProfile " + tempFileName;
                File.WriteAllText(tempFileName, pshScript);
            }
            else 
            {
                var bashScript = "#!/bin/bash \nfor i in {1..100} \ndo \n 	echo David \n	sleep 1 \ndone";

                exec = "bash";
                args = tempFileName;
                File.WriteAllText(tempFileName, bashScript);
                Process.Start("chmod", $"755 {tempFileName}");
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                args,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointData { ProcessCpuUsage = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 10)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ProcessCpuUsage < 0, $"Got {value} {nameof(value.ProcessCpuUsage)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process return invalid data");
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessWriteError_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            string exec;
            string args;
            var jsonCpuUsage = "{\"MachineCpuUsage\":57, \"ProcessCpuUsage\":2.5}";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                jsonCpuUsage = $"\"{jsonCpuUsage.Replace("\"", "`\"")}\"";
                const string error = "\"Big error!\"";

                var pshScript = string.Format(@"
while($TRUE){{
	Write-Host {0}
    Write-Error {1}
	Start-Sleep 1
}}", jsonCpuUsage, error);

                args = "-NoProfile " + tempFileName;
                File.WriteAllText(tempFileName, pshScript);
            }
            else 
            {
                var bashScript = "#!/bin/bash \nfor i in {1..100} \ndo \n 	echo "+ jsonCpuUsage.Replace("\"", "\\\"") + "\n __________ \n	sleep 1 \ndone";

                exec = "bash";
                args = tempFileName;
                File.WriteAllText(tempFileName, bashScript);
                Process.Start("chmod", $"755 {tempFileName}");
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                args,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointData { ProcessCpuUsage = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1 || Math.Abs(57 - value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 10)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ProcessCpuUsage < 0, $"Got {value} {nameof(value.ProcessCpuUsage)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process exited");
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessSendValidJsonWithoutRelevantProperties_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            string exec;
            string args;
            const string jsonCpuUsage = "\"{}\"";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                

                var pshScript = string.Format(@"
while($TRUE){{
	Write-Host {0}
	Start-Sleep 1
}}", jsonCpuUsage);

                args = "-NoProfile " + tempFileName;
                File.WriteAllText(tempFileName, pshScript);
            }
            else
            {
                var bashScript = "#!/bin/bash \nfor i in {1..100} \ndo \n 	echo " + jsonCpuUsage + " \n	sleep 1 \ndone";

                exec = "bash";
                args = tempFileName;
                File.WriteAllText(tempFileName, bashScript);
                Process.Start("chmod", $"755 {tempFileName}");
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                args,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointData { ProcessCpuUsage = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1 || Math.Abs(57 - value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 10)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ProcessCpuUsage < 0, $"Got {value} {nameof(value.ProcessCpuUsage)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process send errors");
            }
        }
    }
}
