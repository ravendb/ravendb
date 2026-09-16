using System;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_26859 : NoDisposalNeeded
    {
        public RavenDB_26859(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.Voron)]
        public void DisablingIoRingUnderExplicitIoRingWriteModeFailsClearly()
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.WriteMode), "IoRing");
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.IoRingQueueSize), "-1");

            var error = Assert.Throws<InvalidOperationException>(() => configuration.Initialize());
            Assert.Contains("Auto", error.Message);
        }

        [RavenTheory(RavenTestCategory.Configuration | RavenTestCategory.Voron)]
        [InlineData("IoRing", "1024")]
        [InlineData("Auto", "-1")]
        [InlineData("Auto", "3")]
        [InlineData("FileIo", "-1")]
        [InlineData("FileIo", "3")]
        public void ValidIoRingConfigurationInitializes(string writeMode, string queueSize)
        {
            var configuration = RavenConfiguration.CreateForServer(null);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.WriteMode), writeMode);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Storage.IoRingQueueSize), queueSize);

            configuration.Initialize();
        }
    }
}
