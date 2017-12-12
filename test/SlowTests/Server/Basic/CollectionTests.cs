using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Raven.Client.ServerWide;
using Raven.Server.Rachis;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Server.Basic
{
    public class CollectionTests : RavenTestBase
    {
        [Fact]
        public async Task CanDeleteCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 1; i <= 10; i++)
                    {
                        await session.StoreAsync(new User { Name = "User " + i }, "users/" + i);
                    }

                    await session.SaveChangesAsync();
                }

                var operation = await store.Operations.SendAsync(new DeleteByQueryOperation(new IndexQuery { Query = "FROM Users" }), CancellationToken.None);
                await operation.WaitForCompletionAsync();

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());

                Assert.Equal(0, stats.CountOfDocuments);
            }
        }

        [Fact]
        public async Task RapidDatabaseDeletionAndRecreation()
        {
            using (var store = GetDocumentStore())
            {
                var name = store.Database;
                var t = store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), @"P:\Dumps\Stackoverflow\users.dump");
                await Task.Delay(TimeSpan.FromSeconds(5));
                var (index, _ ) = await Servers[0].ServerStore.DeleteDatabaseAsync(name, true, null);
                await Servers[0].ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
                (index, _) = await Servers[0].ServerStore.WriteDatabaseRecordAsync(name, new DatabaseRecord(name)
                {
                    Topology = new DatabaseTopology
                    {
                        Members = new List<string> { "A" }
                    }
                }, null);
                await Servers[0].ServerStore.WaitForCommitIndexChange(RachisConsensus.CommitIndexModification.GreaterOrEqual, index);
                await Task.Delay(TimeSpan.FromSeconds(15));
                await t.IgnoreUnobservedExceptions();
            }
        }
    }
}
