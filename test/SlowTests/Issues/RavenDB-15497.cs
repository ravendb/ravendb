using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
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
        public void WaitForIndexesAfterSaveChangesCanExitWhenThrowOnTimeoutIsFalse()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                index.Execute(store);
                store.Maintenance.Send(new DisableIndexOperation(index.IndexName));

                var indexStats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
                Assert.Equal(IndexState.Disabled, indexStats.State);
                Assert.Equal(IndexRunningStatus.Disabled, indexStats.Status);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1"
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5), throwOnTimeout: false);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1"
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(5), throwOnTimeout: true);

                    var error = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.StartsWith("System.TimeoutException", error.Message);
                    Assert.Contains("could not verify that 1 indexes has caught up with the changes as of etag 3", error.Message);
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
