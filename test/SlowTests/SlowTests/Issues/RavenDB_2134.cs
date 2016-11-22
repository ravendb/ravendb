// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2134.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Data;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.SlowTests.Issues
{
    public class RavenDB_2134 : RavenTestBase
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

                WaitForIndexing(store);
                var queryToDelete = new IndexQuery
                {
                    Query = string.Empty
                };

                var operation = store.DatabaseCommands.DeleteByIndex(stats.IndexName, queryToDelete);
                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

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
