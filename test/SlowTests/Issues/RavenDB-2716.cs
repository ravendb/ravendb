using System;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_2716 : RavenTestBase
    {
        public RavenDB_2716(ITestOutputHelper output) : base(output)
        {
        }

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
        public void CanLimitOpsPerSecOnDelete()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 10000; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            Name = "Users #" + i
                        });
                    }
                }

                new Users_ByName().Execute(store);

                Indexes.WaitForIndexing(store);

                var sw = Stopwatch.StartNew();
                var op = store.Operations.Send(new DeleteByQueryOperation(new IndexQuery { Query = "FROM INDEX 'Users/ByName' WHERE startsWith(Name, 'Users')" },
                    new QueryOperationOptions { AllowStale = false, MaxOpsPerSecond = 2000, StaleTimeout = null }));

                op.WaitForCompletion(TimeSpan.FromSeconds(15));
                sw.Stop();

                Assert.True(sw.Elapsed >= TimeSpan.FromSeconds(4));
            }

        }

        [Fact]
        public void CanLimitOpsPerSecOnUpdate()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            Name = "Users #" + i
                        });
                    }
                }

                new Users_ByName().Execute(store);

                Indexes.WaitForIndexing(store);

                var sw = Stopwatch.StartNew();
                var op = store.Operations.Send(new PatchByQueryOperation(new IndexQuery { Query = "FROM INDEX 'Users/ByName' WHERE startsWith(Name, 'Users') UPDATE { this.Test = 'abc'; } " },
                    new QueryOperationOptions { AllowStale = false, MaxOpsPerSecond = 20, StaleTimeout = null }));

                op.WaitForCompletion(TimeSpan.FromSeconds(15));
                sw.Stop();

                Assert.True(sw.Elapsed >= TimeSpan.FromSeconds(4));
            }

        }
    }
}
