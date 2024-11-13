using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests.Server.Replication;
using FastTests.Utils;
using Raven.Client.Documents.Operations.Revisions;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Sparrow;
using Sparrow.Global;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;
public class RavenDB_22840 : ReplicationTestBase
{
    public RavenDB_22840(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Revisions)]
    public async Task RevertRevision_Should_Return_Right_ScannedRevisions_When_Reaches_To_SizeLimitInBytes()
    {
        using (var store = GetDocumentStore())
        {
            var configuration = new RevisionsConfiguration
            {
                Default = new RevisionsCollectionConfiguration
                {
                    Disabled = false,
                }
            };
            await RevisionsHelper.SetupRevisions(store, Server.ServerStore, configuration: configuration);

            DateTime last = default;

            using (var session = store.OpenAsyncSession())
            {
                var person = new Person
                {
                    Name = "Name1"
                };
                await session.StoreAsync(person, "foo/bar");
                await session.SaveChangesAsync();
                last = DateTime.UtcNow;
            }

            using (var session = store.OpenAsyncSession())
            {
                var person = new Person
                {
                    Name = "Name2"
                };
                await session.StoreAsync(person, "foo/bar");
                await session.SaveChangesAsync();
            }

            var db = await Server.ServerStore.DatabasesLandlord.TryGetOrCreateResourceStore(store.Database);
            db.DocumentsStorage.RevisionsStorage.SizeLimitInBytes = 0;

            RevertResult result;
            using (var token = new OperationCancelToken(db.Configuration.Databases.OperationTimeout.AsTimeSpan, db.DatabaseShutdown, CancellationToken.None))
            {
                result = (RevertResult)await db.DocumentsStorage.RevisionsStorage.RevertRevisions(last, TimeSpan.FromMinutes(60), onProgress: null, token: token);
            }

            Assert.Equal(2, result.ScannedRevisions);
        }
    }

    private class Person
    {
        public string Id { get; set; }

        public string Name { get; set; }

        public string AddressId { get; set; }
    }

}
