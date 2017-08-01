// -----------------------------------------------------------------------
//  <copyright file="AsyncSetBasedOps.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
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

        [Fact(Skip = "RavenDB-6274")]
        public async Task AwaitAsyncPatchByIndexShouldWork()
        {
            using (var store = GetDocumentStore(modifyDatabaseRecord: document => document.Settings[RavenConfiguration.GetKey(x => x.Core.RunInMemory)] = "false"))
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

                await (await store.Operations.SendAsync(new PatchByIndexOperation(
                    new IndexQuery { Query = $"FROM INDEX '{stats.IndexName}'" },
                    new PatchRequest
                    {
                        Script = "this.FullName = this.FirstName + ' ' + this.LastName;"
                    }
                ),CancellationToken.None))
                .WaitForCompletionAsync(TimeSpan.FromSeconds(15));

                using (var db = store.OpenAsyncSession())
                {
                    var lastUser = await db.LoadAsync<User>(lastUserId);
                    Assert.NotNull(lastUser.FullName);
                }
            }
        }

    }
}
