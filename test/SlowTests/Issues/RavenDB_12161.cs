using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Threading;
using FastTests;
using Microsoft.Extensions.Logging;
using NetTopologySuite.Utilities;
using Raven.Server.Config.Settings;
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
            var exec = "";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                const string jsonCpuUsage = "\"{`\"MachineCpuUsage`\":57, `\"ActiveCores`\":2.5}\"";

                var pshScript = string.Format(@"
while($TRUE){{
	Write-Host {0}
	Start-Sleep 1
}}", jsonCpuUsage);

                File.WriteAllText(tempFileName, pshScript);
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                "-NoProfile " + tempFileName,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointRawData { ActiveCores = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 100)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.Equal(57, value.MachineCpuUsage);
                Assert.Equal(2.5, value.ActiveCores);
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessExit_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var exec = "";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                const string pshScript = "";

                File.WriteAllText(tempFileName, pshScript);
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                "-NoProfile " + tempFileName,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointRawData{ ActiveCores = 0, MachineCpuUsage = 0};
                while (Math.Abs(value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 100)
                    {
                        throw new TimeoutException();
                    }
                     value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ActiveCores < 0, $"Got {value} {nameof(value.ActiveCores)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process exited");
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessReturnInvalidData_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var exec = "";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";

                var pshScript = @"
while($TRUE){{
	Write-Host David
	Start-Sleep 1
}}";

                File.WriteAllText(tempFileName, pshScript);
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                "-NoProfile " + tempFileName,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointRawData { ActiveCores = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 100)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ActiveCores < 0, $"Got {value} {nameof(value.ActiveCores)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process exited");
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessWriteError_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var exec = "";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                const string jsonCpuUsage = "\"{`\"MachineCpuUsage`\":57, `\"ActiveCores`\":2.5}\"";
                const string error = "\"Big error!\"";

                var pshScript = string.Format(@"
while($TRUE){{
	Write-Host {0}
    Write-Error {1}
	Start-Sleep 1
}}", jsonCpuUsage, error);

                File.WriteAllText(tempFileName, pshScript);
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                "-NoProfile " + tempFileName,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointRawData { ActiveCores = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1 || Math.Abs(57 - value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 100)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ActiveCores < 0, $"Got {value} {nameof(value.ActiveCores)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process exited");
            }
        }

        [Fact]
        public void GetCpuUsage_WhenExtensionPointProcessSendValidJsonWithoutRelevantProperties_ShouldDisposeAndSetDataToNegative()
        {
            var tempFileName = Path.Combine(Path.GetTempPath(), Path.ChangeExtension(Guid.NewGuid().ToString(), ".ps1"));
            var exec = "";
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                exec = "PowerShell";
                const string jsonCpuUsage = "\"{}\"";

                var pshScript = string.Format(@"
while($TRUE){{
	Write-Host {0}
	Start-Sleep 1
}}", jsonCpuUsage);

                File.WriteAllText(tempFileName, pshScript);
            }

            var logger = LoggingSource.Instance.GetLogger<RavenDB_12161>("");
            using (var extensionPoint = new CpuUsageExtensionPoint(
                new JsonContextPool(),
                exec,
                "-NoProfile " + tempFileName,
                logger,
                Server.ServerStore.NotificationCenter))
            {
                extensionPoint.Start();

                var startTime = DateTime.Now;
                var value = new ExtensionPointRawData { ActiveCores = 0, MachineCpuUsage = 0 };
                while (Math.Abs(value.MachineCpuUsage) < 0.1 || Math.Abs(57 - value.MachineCpuUsage) < 0.1)
                {
                    if ((DateTime.Now - startTime).Seconds > 100)
                    {
                        throw new TimeoutException();
                    }
                    value = extensionPoint.Data;
                }

                Assert.True(value.MachineCpuUsage < 0, $"Got {value} {nameof(value.MachineCpuUsage)} should get negative error value");
                Assert.True(value.ActiveCores < 0, $"Got {value} {nameof(value.ActiveCores)} should get negative error value");

                Assert.True(extensionPoint.IsDisposed, "Should dispose the extension point object if the process exited");
            }
        }
    }
}
