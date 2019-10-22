using System;
using System.IO;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_7886 : NoDisposalNeeded
    {
        public RavenDB_7886(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GivenNonExistingFileAsCustomConfigPathConfigurationLoadingShouldThrow()
        {
            try
            {
                var configuration = RavenConfiguration.CreateForServer(null, "thisIsANonExistentSettingsFile.json");
                throw new Exception("Configuration loading should have thrown.");
            }
            catch (FileNotFoundException exc)
            {
                Assert.Equal("Custom configuration file has not been found.", exc.Message);
            }
        }
    }
}
