using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_20915 : RavenTestBase
{
    public RavenDB_20915(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Configuration)]
    public async Task Allow_To_Set_MaxIdleTime_Per_Database()
    {
        UseNewLocalServer(new Dictionary<string, string>
        {
            { RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle), "1" }
        });

        using (var store = GetDocumentStore(new Options
        {
            RunInMemory = false,
            ModifyDatabaseRecord = r => r.Settings[RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "1"
        }))
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "HR" });
                await session.SaveChangesAsync(); // update last work time
            }

            Server.ServerStore.DatabasesLandlord.ForTestingPurposesOnly().ShouldFetchIdleStateImmediately = true;

            Assert.False(WaitForValue(() => Server.ServerStore.DatabasesLandlord.DatabasesCache.TryGetValue(store.Database, out _), false));
        }
    }
}
