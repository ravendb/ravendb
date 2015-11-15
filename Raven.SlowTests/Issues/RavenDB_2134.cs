// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2134.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;

using Raven.Abstractions.Data;
using Raven.Tests.Common;
using Raven.Tests.Common.Dto;

using Xunit;

namespace Raven.SlowTests.Issues
{
    public class RavenDB_2134 : RavenTest
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = NewDocumentStore(requestedStorage: "voron"))
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    for (int i = 0; i < 20000; i++)
                    {
                        bulkInsert.Store(new User { Id = i.ToString(), Name = "Name" + i });
                    }
                }

                WaitForIndexing(store);
                var queryToDelete = new IndexQuery
                                    {
                                        Query = "Tag:Users"
                                    };

                var operation = store.DatabaseCommands.DeleteByIndex("Raven/DocumentsByEntityName", queryToDelete);
                operation.WaitForCompletion();

                using (var session = store.OpenSession())
                {
                    var count = session
                        .Query<User>("Raven/DocumentsByEntityName")
                        .Customize(x => x.WaitForNonStaleResults())
                        .Count();

                    Assert.Equal(0, count);
                }
            }
        }
    }
}
