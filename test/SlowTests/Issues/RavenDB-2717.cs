using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2717 : RavenTestBase
    {
        private class User
        {
            public string Name { get; set; }
        }

        private class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users
                               select new { user.Name };
            }
        }

        [Fact]
        public void FailWaitOnStaleTimeout()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            Name = "Users #" + i
                        });
                    }
                }

                store.Admin.Send(new StopIndexingOperation());

                var usersByNameIndex = new Users_ByName();
                usersByNameIndex.Execute(store);

                var exception = Assert.Throws<RavenException>(() =>
                {
                    var op = store.Operations.Send(new DeleteByIndexOperation(
                        new IndexQuery { Query = "FROM INDEX 'Users/ByName' WHERE startsWith(Name, 'Users')" },
                        new QueryOperationOptions { AllowStale = false, MaxOpsPerSecond = null, StaleTimeout = TimeSpan.FromMilliseconds(10) }));

                    op.WaitForCompletion(TimeSpan.FromSeconds(15));
                });

                Assert.Contains("Cannot perform bulk operation. Query is stale.", exception.Message);
            }
        }
    }
}
