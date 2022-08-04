using System;
using System.Threading.Tasks;
using System.Xml.Schema;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15143 : RavenTestBase
    {
        public RavenDB_15143(ITestOutputHelper output) : base(output)
        {
        }

        public class Locker
        {
            public string ClientId;
        }

        public class Command
        {
            public string Id;
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task CanUseCreateCmpXngToInSessionWithNoOtherChanges(Options options)
        {
            using var store = GetDocumentStore(options);

            using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
            {
                await session.StoreAsync(new Command(), "cmd/239-A");
                await session.SaveChangesAsync();
            }
            using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
            {
                var result = await session.Query<Command>()
                    .Include(include => include.IncludeCompareExchangeValue(x => x.Id))
                    .FirstOrDefaultAsync();

                Assert.NotNull(result);
                
                var locker = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<Locker>("cmd/239-A");
                Assert.Null(locker);

                locker = session.Advanced.ClusterTransaction.CreateCompareExchangeValue("cmd/239-A", new Locker
                {
                    ClientId = "a"
                });

                locker.Metadata["@expires"] = DateTime.UtcNow.AddMinutes(2);

                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession(new SessionOptions {TransactionMode = TransactionMode.ClusterWide}))
            {
                var smile = await session.Advanced.ClusterTransaction.GetCompareExchangeValueAsync<string>("cmd/239-A");
                Assert.NotNull(smile);
            }
        }
    }
}
