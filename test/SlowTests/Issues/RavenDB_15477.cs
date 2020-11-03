using System.Linq;
using System.Threading;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Json;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15477 : RavenTestBase
    {
        public RavenDB_15477(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanDeserializeDatabaseInfoForDisabledDb()
        {
            using (var store = GetDocumentStore())
            {
                new UserIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "egor" });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                store.Maintenance.Server.Send(new ToggleDatabasesStateOperation(store.Database, true));

                var client = store.GetRequestExecutor().HttpClient;
                var response = client.GetAsync(store.Urls.First() + $"/databases?name={store.Database}", CancellationToken.None).Result;
                var result = response.Content.ReadAsStringAsync().Result;

                using (var ctx = JsonOperationContext.ShortTermSingleUse())
                {
                    using var bjro = ctx.Sync.ReadForMemory(result, "test");
                    Assert.True(bjro.TryGetMember(nameof(DatabaseInfo.IndexingStatus), out var indexingStatus));
                    Assert.Null(indexingStatus);
                    var dbInfo = JsonDeserializationServer.DatabaseInfo(bjro);
                    Assert.Equal(IndexRunningStatus.Running, dbInfo.IndexingStatus);
                }
            }
        }

        private class UserIndex : AbstractIndexCreationTask<User, UserIndex.Result>
        {
            public UserIndex()
            {
                Map = users => from user in users
                               select new
                               {
                                   user.Name
                               };

            }

            public class Result
            {
                public string Name { get; set; }
            }
        }
    }
}
