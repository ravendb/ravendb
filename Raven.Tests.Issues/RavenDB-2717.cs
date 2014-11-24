using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Common;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_2717 : RavenTest
    {
        public class User
        {
            public String Name { get; set; }
        }

        public class Users_ByName : AbstractIndexCreationTask<User>
        {
            public Users_ByName()
            {
                Map = users => from user in users
                               select new { user.Name };
            }
        }

        [Fact]
        public void CanWaitOnStaleTimeout()
        {
            using (var store = NewDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            Name = "Users #" + i
                        }, "users/" + i);
                    }
                }

                store.DatabaseCommands.Admin.StopIndexing();

                var usersByNameIndex = new Users_ByName();
                usersByNameIndex.Execute(store);

                var waits = 0;
	            var mres = new ManualResetEventSlim();
                SystemTime.WaitCalled = ms =>
                {
	                waits++;
					mres.Set();
					Thread.Sleep(10);
                };

	            var op = store.DatabaseCommands.DeleteByIndex("Users/ByName",
		            new IndexQuery {Query = "Name:Users*"}
		            , new BulkOperationOptions
		            {
			            AllowStale = false,
			            MaxOpsPerSec = null,
			            StaleTimeout = TimeSpan.FromSeconds(15)
		            });
	            mres.Wait();

                store.DatabaseCommands.Admin.StartIndexing();

                op.WaitForCompletion();
                using (var session = store.OpenSession())
                {
                    Assert.True(waits > 0);

	                Assert.Empty(session.Query<User>());
                }
                            
            }

        }
        [Fact]
        public void FailWaitOnStaleTimeout()
        {
            using (var store = NewDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        bulkInsert.Store(new User
                        {
                            Name = "Users #" + i
                        }, "users/" + i);
                    }
                }

                store.DatabaseCommands.Admin.StopIndexing();

                var usersByNameIndex = new Users_ByName();
                usersByNameIndex.Execute(store);

                var waits = 0;
                SystemTime.WaitCalled = ms => waits++;
                bool exceptionThrown = false;
                try
                {
                    var op = store.DatabaseCommands.DeleteByIndex("Users/ByName",
                        new IndexQuery {Query = "Name:Users*"}
                        , new BulkOperationOptions {AllowStale = false, MaxOpsPerSec = null, StaleTimeout = TimeSpan.FromMilliseconds(1)});

                    store.DatabaseCommands.Admin.StartIndexing();

                    op.WaitForCompletion();
                }
                catch (InvalidOperationException e)
                {
					Assert.Contains("Operation failed: Bulk operation cancelled because the index is stale", e.Message);
                    exceptionThrown = true;
                }
                Assert.True(exceptionThrown);

            }

        }
    }
}
