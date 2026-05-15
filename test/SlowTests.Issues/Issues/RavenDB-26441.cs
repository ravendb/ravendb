using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Config;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26441 : RavenTestBase
{
    public RavenDB_26441(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Replication)]
    public async Task UnloadDatabase_WhenDatabaseShutdownRegisterCallerThrow_ShouldBeAbleToLoad()
    {
        using var server = GetNewServer(new ServerCreationOptions
        {
            CustomSettings = new Dictionary<string, string>
            {
                [RavenConfiguration.GetKey(x => x.Databases.MaxIdleTime)] = "1",
                [RavenConfiguration.GetKey(x => x.Databases.FrequencyToCheckForIdle)] = "1",
                [RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
            }
        });

        string databaseName;
        using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, DeleteDatabaseOnDispose = false }))
        {
            databaseName = store.Database;
            var db = await GetDatabase(server, databaseName);
            db.DatabaseShutdown.Register(() => throw new Exception("Artificial exception"));
        }

        await AssertWaitForValueAsync(() => Task.FromResult(server.ServerStore.IdleDatabases.Count), 1);

        using (var store = GetDocumentStore(new Options { Server = server, RunInMemory = false, CreateDatabase = false, ModifyDatabaseName = _ => databaseName }))
        {
            using var session = store.OpenAsyncSession();
            await session.LoadAsync<object>("randomId"); // forces us to load the database, and if the exception from the DatabaseShutdown register is not handled, it will fail to load
        }
    }
}
