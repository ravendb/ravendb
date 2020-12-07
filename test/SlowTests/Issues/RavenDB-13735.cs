using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Refresh;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13735 : RavenTestBase
    {
        public RavenDB_13735(ITestOutputHelper output) : base(output)
        {
        }

        private async Task SetupRefresh(DocumentStore store)
        {
            var config = new RefreshConfiguration
            {
                Disabled = false,
                RefreshFrequencyInSec = 100,
            };

            var result = await store.Maintenance.SendAsync(new ConfigureRefreshOperation(config));
            await Server.ServerStore.Cluster.WaitForIndexNotification(result.RaftCommandIndex ?? 1, TimeSpan.FromMinutes(1));
        }

        [Fact]
        public async Task RefreshWillUpdateDocumentChangeVector()
        {
            using (var store = GetDocumentStore())
            {
                await SetupRefresh(store);
                string expectedChangeVector = null;
                using (var session = store.OpenAsyncSession())
                {
                    var user = new { Name = "Oren" };
                    await session.StoreAsync(user, "users/1-A");

                    session.Advanced.GetMetadataFor(user)["@refresh"] = DateTime.UtcNow.AddHours(-1).ToString("o");

                    await session.SaveChangesAsync();

                    expectedChangeVector = session.Advanced.GetChangeVectorFor(user);
                }

                var database = await GetDocumentDatabaseInstanceFor(store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                var expiredDocumentsCleaner = database.ExpiredDocumentsCleaner;
                await expiredDocumentsCleaner.RefreshDocs();

                using (var session = store.OpenAsyncSession())
                {
                    var user = await session.LoadAsync<object>("users/1-A");
                    Assert.NotNull(user);
                    var actualChangeVector = session.Advanced.GetChangeVectorFor(user);

                    Assert.NotEqual(expectedChangeVector, actualChangeVector);
                }

            }
        }
    }
}
