using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Server.Config.Settings;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_17753 : RavenTestBase
{
    public RavenDB_17753(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Voron)]
    public async Task StorageTempPathIsUsedBySystemServerStore()
    {
        var storageTempPath = new PathSetting(NewDataPath(suffix: "_17753_temp"));

        using (var server = GetNewServer(new ServerCreationOptions
               {
                   RunInMemory = false,
                   CustomSettings = new Dictionary<string, string>
                   {
                       { RavenConfiguration.GetKey(x => x.Storage.TempPath), storageTempPath.FullPath}
                   }
               }))
        {
            var systemTempPath = server.ServerStore._env.Options.TempPath;

            Assert.Equal(storageTempPath.Combine("System").FullPath, systemTempPath.FullPath);

            using (var dbStore = GetDocumentStore(new Options
                   {
                       Server = server
                   }))
            {
                var db = await GetDatabase(server, dbStore.Database);

                var dbTempPath = db.DocumentsStorage.Environment.Options.TempPath;

                Assert.Equal(storageTempPath.Combine("Databases").Combine(dbStore.Database).FullPath, dbTempPath.FullPath);
            }
        }
    }
}
