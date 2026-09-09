using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using System.Text;
using System.IO;
using System.Threading.Tasks;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests
{
    public class RuntimeFrameworkVersionMatcherTests : EmbeddedTestBase
    {
        [Fact]
        public async Task MatchTest1()
        {
            var options = new ServerOptions();

            var defaultFrameworkVersion = ServerOptions.Default.FrameworkVersion;
            Assert.True(defaultFrameworkVersion.EndsWith(RuntimeFrameworkVersionMatcher.GreaterOrEqual.ToString()));

            var expectedVersion = new Version(defaultFrameworkVersion.Substring(0, defaultFrameworkVersion.Length - 1));
            var actualVersion = new Version(await RuntimeFrameworkVersionMatcher.MatchAsync(options));

            Assert.True(actualVersion.CompareTo(expectedVersion) >= 0);

            options.FrameworkVersion = null;
            Assert.Null(await RuntimeFrameworkVersionMatcher.MatchAsync(options));

            options = new ServerOptions();

            var frameworkVersion = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion(options.FrameworkVersion)
            {
                Patch = null
            };

            options.FrameworkVersion = frameworkVersion.ToString();
            var match = await RuntimeFrameworkVersionMatcher.MatchAsync(options);
            Assert.NotNull(match);
            var matchFrameworkVersion = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion(match);
            Assert.True(matchFrameworkVersion.Major.HasValue);
            Assert.True(matchFrameworkVersion.Minor.HasValue);
            Assert.True(matchFrameworkVersion.Patch.HasValue);

            Assert.True(frameworkVersion.Match(matchFrameworkVersion));

            options = new ServerOptions
            {
                DotNetPath = Path.GetTempFileName(),
                FrameworkVersion = frameworkVersion.ToString()
            };

            var e = await Assert.ThrowsAsync<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.MatchAsync(options));
            Assert.Contains("Unable to execute dotnet to retrieve list of installed runtimes", e.Message);
        }

        [Fact]
        public void MatchTest2()
        {
            var runtimes = GetRuntimes();

            var runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.1");
            Assert.Equal("3.1.1", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.11");
            Assert.Equal("2.1.11", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.x");
            Assert.Equal("3.1.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.x");
            Assert.Equal("3.2.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.x.x");
            Assert.Equal("3.2.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.x");
            Assert.Equal("5.0.4", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("x");
            Assert.Equal("5.0.4", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.x-rc.2.20475.17");
            Assert.Equal("5.0.0-rc.2.20475.17", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.x");
            var e = Assert.Throws<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));
            Assert.Contains("Could not find a matching runtime for '6.x.x'", e.Message);
        }

        [Fact]
        public void MatchTest3()
        {
            var runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.0-rc");
            Assert.Equal("3.1.0-rc", runtime.ToString());

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.0-rc.2.20475.17");
            Assert.Equal("5.0.0-rc.2.20475.17", runtime.ToString());
        }

        [Fact]
        public void MatchTest4()
        {
            var runtimes = GetRuntimes();

            var runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.1+");
            Assert.Equal("3.1.1+", runtime.ToString());
            Assert.Equal("3.1.3", RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));

            runtime = new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.4+");
            Assert.Equal("3.1.4+", runtime.ToString());
            var e = Assert.Throws<InvalidOperationException>(() => RuntimeFrameworkVersionMatcher.Match(runtime, runtimes));
            Assert.Contains("Could not find a matching runtime for '3.1.4+'. Available runtimes:", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.0.0+-preview.6.21352.12"));
            Assert.Equal("Cannot set 'Patch' with value '0+' because '+' is not allowed when Suffix ('preview.6.21352.12') is set.", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6+"));
            Assert.Equal("Cannot set 'Major' with value '6+' because '+' is not allowed.", e.Message);

            e = Assert.Throws<InvalidOperationException>(() => new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1+"));
            Assert.Equal("Cannot set 'Minor' with value '1+' because '+' is not allowed.", e.Message);
        }

#if NETCOREAPP
        [Theory]
        [InlineData("diagnosis")]
        [InlineData("command")]
        [InlineData("exit code")]
        [InlineData("stderr")]
        public async Task Should_report_runtime_discovery_failure_when_dotnet_host_is_incomplete(string diagnostic)
        {
            ServerOptions options = CopyServerAndCreateOptions();
            options.DotNetPath = CopyDotNetHost();

            InvalidOperationException error = await GetStartupFailure(options);

            // Each diagnostic is checked independently so the red run reaches every assertion.
            switch (diagnostic)
            {
                case "diagnosis":
                    Assert.DoesNotContain("Could not find a matching runtime", error.Message);
                    Assert.Contains("Unable to discover installed .NET runtimes", error.Message);
                    break;
                case "command":
                    Assert.Contains(options.DotNetPath, error.Message);
                    Assert.Contains("--info", error.Message);
                    break;
                case "exit code":
                    Assert.Contains(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "-2147450749 (0x80008083)" : "131 (0x00000083)", error.Message);
                    break;
                case "stderr":
                    Assert.Contains("host", error.Message);
                    Assert.Contains("fxr", error.Message);
                    break;
            }
        }

        [WindowsFact]
        public async Task Should_report_windows_loader_failure_with_empty_output()
        {
            ServerOptions options = CopyServerAndCreateOptions();
            options.DotNetPath = CopyDotNetHost();

            // Change an import in a private copy, leaving the installed host untouched. Windows
            // must fail in the real loader because the required DLL does not exist.
            byte[] executable = File.ReadAllBytes(options.DotNetPath);
            int importOffset = Encoding.ASCII.GetString(executable).IndexOf("KERNEL32.dll\0", StringComparison.OrdinalIgnoreCase);
            Assert.True(importOffset >= 0, "The Windows dotnet host must import KERNEL32.dll.");
            Encoding.ASCII.GetBytes("RDB27484.dll").CopyTo(executable, importOffset);
            File.WriteAllBytes(options.DotNetPath, executable);

            // Verify the native failure itself before exercising the public embedded lifecycle.
            using (Process child = Process.Start(new ProcessStartInfo(options.DotNetPath, "--info")
                   { UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true }))
            {
                Task<string> output = child.StandardOutput.ReadToEndAsync();
                Task<string> error = child.StandardError.ReadToEndAsync();
                Assert.True(child.WaitForExit(30_000));
                Assert.Equal(unchecked((int)0xC0000135), child.ExitCode);
                Assert.Empty(await output);
                Assert.Empty(await error);
            }

            InvalidOperationException failure = await GetStartupFailure(options);
            Assert.Contains("-1073741515 (0xC0000135)", failure.Message);
            Assert.Contains("STATUS_DLL_NOT_FOUND", failure.Message);
            Assert.Contains(options.DotNetPath, failure.Message);
        }

        [Fact]
        public async Task Should_preserve_a_genuine_runtime_version_mismatch()
        {
            ServerOptions options = CopyServerAndCreateOptions();
            options.FrameworkVersion = "999.0.0+";

            InvalidOperationException error = await GetStartupFailure(options);
            Assert.Contains("Could not find a matching runtime for '999.0.0+'. Available runtimes:", error.Message);
            Assert.Contains(Environment.NewLine + "- ", error.Message);
        }

        private static async Task<InvalidOperationException> GetStartupFailure(ServerOptions options)
        {
            var embedded = new EmbeddedServer();
            InvalidOperationException startupError = null;
            try
            {
                embedded.StartServer(options);
                startupError = await Assert.ThrowsAsync<InvalidOperationException>(() => embedded.GetServerUriAsync());
                return startupError;
            }
            finally
            {
                try
                {
                    embedded.Dispose();
                }
                catch (AggregateException error) when (startupError != null && error.InnerExceptions.Count == 1 && ReferenceEquals(error.InnerException, startupError))
                {
                    // Dispose also observes the faulted startup task.
                    // Do not let the same exception hide the diagnostic assertion this regression is checking.
                }
            }
        }

        private string CopyDotNetHost()
        {
            string runtimeDirectory = RuntimeEnvironment.GetRuntimeDirectory();
            string installation = Directory.GetParent(runtimeDirectory.TrimEnd(Path.DirectorySeparatorChar)).Parent.Parent.FullName;
            string executable = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "dotnet.exe" : "dotnet";
            string directory = Path.Combine(NewDataPath(), "incomplete dotnet installation");
            Directory.CreateDirectory(directory);
            string copy = Path.Combine(directory, executable);
            File.Copy(Path.Combine(installation, executable), copy);
            return copy;
        }

        private sealed class WindowsFactAttribute : FactAttribute
        {
            public WindowsFactAttribute()
            {
                if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows) == false)
                    Skip = "This test exercises the Windows native DLL loader.";
            }
        }
#endif
        private static List<RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion> GetRuntimes()
        {
            return new()
            {
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.4"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.1.11"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.2.0"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("2.2.1"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.0"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.1"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.2"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.1.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("3.2.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.0-rc.2.20475.17"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.3"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.4"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.0.0-preview.6.21352.12"),
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("6.0.0-rc.1.21451.13")
            };
        }
    }
}
