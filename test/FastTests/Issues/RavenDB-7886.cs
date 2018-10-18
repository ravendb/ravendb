using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using Raven.Server.Config;
using Raven.Server.ServerWide;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_7886 : NoDisposalNeeded
    {
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
