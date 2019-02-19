// -----------------------------------------------------------------------
//  <copyright file="AsyncSetBasedOps.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Xunit;

namespace SlowTests.SlowTests.Bugs
{
    public class AsyncSetBasedOps : RavenTestBase
    {
        private class User
        {
            public string FirstName;
#pragma warning disable 414,649
            public string LastName;
            public string FullName;
#pragma warning restore 414,649
        }

        [Fact]
        public async Task AwaitAsyncPatchByIndexShouldWork()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"
            }))
            {
                string lastUserId = null;

                QueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.FirstName == "John")
                        .ToList();
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 1000 * 10; i++)
                    {
                        lastUserId = await bulkInsert.StoreAsync(
                            new User
                            {
                                FirstName = "First #" + i,
                                LastName = "Last #" + i
                            }
                        );
                    }
                }

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(5));

                await (await store.Operations.SendAsync(new PatchByQueryOperation(
                    new IndexQuery { Query = $"FROM INDEX '{stats.IndexName}' UPDATE {{ this.FullName = this.FirstName + ' ' + this.LastName; }}" }
                )))
                .WaitForCompletionAsync(TimeSpan.FromSeconds(60));

                using (var db = store.OpenAsyncSession())
                {
                    var lastUser = await db.LoadAsync<User>(lastUserId);
                    Assert.NotNull(lastUser.FullName);
                }
            }
        }

    }
}
