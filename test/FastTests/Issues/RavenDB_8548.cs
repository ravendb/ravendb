using System;
using Raven.Server.Config;
using Sparrow.Logging;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_8548 : NoDisposalNeeded
    {
        public RavenDB_8548(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            Validate("RAVEN_Logs_Mode");
            Validate("RAVEN.Logs.Mode");            
        }

        private static void Validate(string key)
        {
            Environment.SetEnvironmentVariable(key, LogMode.Information.ToString());

            try
            {
                var configuration = RavenConfiguration.CreateForServer("test");

                configuration.Initialize();

                Assert.Equal(LogMode.Information, configuration.Logs.Mode);
            }
            finally
            {
                Environment.SetEnvironmentVariable(key, null);
            }
        }
    }
}
