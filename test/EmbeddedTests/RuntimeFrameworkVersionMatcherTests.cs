using System;
using System.Collections.Generic;
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
            Assert.Equal(ServerOptions.Default.FrameworkVersion, await RuntimeFrameworkVersionMatcher.MatchAsync(options));

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
                new RuntimeFrameworkVersionMatcher.RuntimeFrameworkVersion("5.0.4")
            };
        }
    }
}
