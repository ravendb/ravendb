using Raven.Server.Config;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_9348 : NoDisposalNeeded
    {
        [Fact]
        public void IfConfigurationKeyIsStringArrayThenItShouldSupportValuesWithSemicolonPropely()
        {
            var configuration = RavenConfiguration.CreateForServer("Foo");
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Core.ServerUrls), "http://123.123.123.123:10105;http://123.123.123.124:10105");
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Core.PublicServerUrl), "http://public.url.com");
            configuration.Initialize();

            Assert.NotNull(configuration.Core.ServerUrls);
            Assert.Equal(2, configuration.Core.ServerUrls.Length);
            Assert.Contains("http://123.123.123.123:10105", configuration.Core.ServerUrls);
            Assert.Contains("http://123.123.123.124:10105", configuration.Core.ServerUrls);
        }
    }
}
