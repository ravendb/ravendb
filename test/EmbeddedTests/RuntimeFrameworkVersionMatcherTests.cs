using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.InteropServices;
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
        [Fact]
        public void Should_report_failed_dotnet_runtime_discovery()
        {
            string runtimeDirectory = RuntimeEnvironment.GetRuntimeDirectory().TrimEnd(Path.DirectorySeparatorChar);
            string installation = Directory.GetParent(runtimeDirectory).Parent.Parent.FullName;
            string executable = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "dotnet.exe" : "dotnet";
            string directory = NewDataPath();
            Directory.CreateDirectory(directory);
            string dotnetPath = Path.Combine(directory, executable);
            // A real host without its host/fxr installation fails before it can list runtimes.
            File.Copy(Path.Combine(installation, executable), dotnetPath);
            ServerOptions options = CopyServerAndCreateOptions();
            options.DotNetPath = dotnetPath;

            AggregateException error = Assert.Throws<AggregateException>(() =>
            {
                // Dispose observes the asynchronous startup failure using the existing contract.
                using var embedded = new EmbeddedServer();
                embedded.StartServer(options);
            });
            InvalidOperationException startupError = Assert.IsType<InvalidOperationException>(error.InnerException);

            Assert.DoesNotContain("Could not find a matching runtime", startupError.Message);
            Assert.Contains("Unable to discover installed .NET runtimes", startupError.Message);
            Assert.Contains($"Command: \"{dotnetPath}\" --info", startupError.Message);
            Assert.Contains(RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "Exit code: -2147450749 (0x80008083)" : "Exit code: 131 (0x00000083)", startupError.Message);
            Assert.Contains("Standard output:" + Environment.NewLine + "<empty>", startupError.Message);
            Assert.Contains("Standard error:", startupError.Message);
            Assert.Contains("host", startupError.Message);
            Assert.Contains("fxr", startupError.Message);
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
