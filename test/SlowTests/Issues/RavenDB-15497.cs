using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Exceptions;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15497 : RavenTestBase
    {
        public RavenDB_15497(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WaitForIndexesAfterSaveChangesCanExitWhenThrowOnTimeoutIsFalse()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                await index.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1",
                        Count = 3
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(3), throwOnTimeout: false);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                const int timeoutInMs = 2_000;
                (await GetDatabase(store.Database)).IndexStore.GetIndex(index.IndexName).ForTestingPurposesOnly().BeforeStartingNewBatch = () =>
                {
                    Thread.Sleep(timeoutInMs + 100);
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1"
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromMilliseconds(timeoutInMs), throwOnTimeout: true);

                    var error = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.StartsWith("System.TimeoutException", error.Message);
                    Assert.Contains("could not verify that 1 indexes has caught up with the changes as of etag", error.Message);
                }
            }
        }

        private class Index : AbstractIndexCreationTask<User>
        {
            public Index()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }
    }
}
