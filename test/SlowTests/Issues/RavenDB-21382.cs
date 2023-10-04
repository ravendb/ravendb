using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Graph;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_21382 : ReplicationTestBase
    {
        public RavenDB_21382(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task EnforceConfigurationShouldntThrowNreInDeletedDocWithNoTombstone()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = r => r.Settings["Tombstones.CleanupIntervalInMin"] = int.MaxValue.ToString()
            });
            var configuration = new RevisionsConfiguration { Default = new RevisionsCollectionConfiguration { Disabled = false, } };
            await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

            var user = new User { Id = "Users/1-A", Name = "Shahar" };
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(user);
                await session.SaveChangesAsync();

                for (int i = 1; i <= 10; i++)
                {
                    (await session.LoadAsync<User>(user.Id)).Name = $"Shahar{i}";
                    await session.SaveChangesAsync();
                }

                session.Delete(user.Id);
                await session.SaveChangesAsync();
            }

            var database = await Databases.GetDocumentDatabaseInstanceFor(store);
            using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
            {
                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                    Assert.Equal(1, count);
                }

                await database.TombstoneCleaner.ExecuteCleanup();

                using (context.OpenReadTransaction())
                {
                    var count = database.DocumentsStorage.GetTombstonesFrom(context, "Users", 0, 0, 128).Count();
                    Assert.Equal(0, count);
                }

            }

            using (var token = new OperationCancelToken(database.Configuration.Databases.OperationTimeout.AsTimeSpan, database.DatabaseShutdown, CancellationToken.None))
                await database.DocumentsStorage.RevisionsStorage.EnforceConfigurationAsync(_ => { }, false, token: token);

        }
    }
}
