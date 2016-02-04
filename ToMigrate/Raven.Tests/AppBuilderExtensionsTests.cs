using System.IO;
using Microsoft.Owin.Builder;
using Owin;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Tests.Common;

using Xunit;
using Raven.Abstractions.Data;

namespace Raven.Tests
{
    public class AppBuilderExtensionsTests : RavenTest
    {
        [Fact]
        public void When_HostOnAppDisposing_key_not_exist_then_should_not_throw()
        {
            string path = NewDataPath();
            var configuration = new RavenConfiguration();

            configuration.SetSetting(RavenConfiguration.GetKey(x => x.Core.DataDirectory), path);
            configuration.SetSetting(RavenConfiguration.GetKey(x => x.FileSystem.DataDirectory), Path.Combine(path, "FileSystem"));

            configuration.Initialize();

            using (var options = new RavenDBOptions(configuration))
            {
                Assert.DoesNotThrow(() => new AppBuilder().UseRavenDB(options));
            }
            
        }
    }
}
