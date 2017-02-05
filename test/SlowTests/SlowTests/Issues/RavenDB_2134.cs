// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2134.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.NewClient.Client;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Operations.Databases.Documents;
using Xunit;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_2134 : RavenNewTestBase
    {
        [Fact]
        public async Task ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                RavenQueryStatistics stats;
                using (var session = store.OpenSession())
                {
                    session.Query<User>()
                        .Statistics(out stats)
                        .Where(x => x.Name == "John")
                        .ToList();
                }

                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 20000; i++)
                    {
                        await bulkInsert.StoreAsync(new User { Id = i.ToString(), Name = "Name" + i });
                    }
                }

                WaitForIndexing(store,timeout: TimeSpan.FromMinutes(2));
                var queryToDelete = new IndexQuery(store.Conventions)
                {
                    Query = string.Empty
                };

                var operation = store.Operations.Send(new DeleteByIndexOperation(stats.IndexName, queryToDelete));
                operation.WaitForCompletion(TimeSpan.FromMinutes(2));

                WaitForIndexing(store, timeout: TimeSpan.FromMinutes(2));

                using (var session = store.OpenSession())
                {
                    var count = session
                        .Query<User>(stats.IndexName)
                        .Customize(x => x.WaitForNonStaleResults())
                        .Count();

                    Assert.Equal(0, count);
                }
            }
        }
    }
}
