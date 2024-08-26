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
            Validate("RAVEN_Logs_MinLevel");
            Validate("RAVEN.Logs.MinLevel");            
        }

        private static void Validate(string key)
        {
            Environment.SetEnvironmentVariable(key, LogLevel.Info.ToString());

            try
            {
                var configuration = RavenConfiguration.CreateForServer("test");

                configuration.Initialize();

                Assert.Equal(LogLevel.Info, configuration.Logs.MinLevel);
            }
            finally
            {
                Environment.SetEnvironmentVariable(key, null);
            }
        }
    }
}
