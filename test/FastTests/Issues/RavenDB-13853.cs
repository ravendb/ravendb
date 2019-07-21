using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using FastTests.Graph;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Sparrow.Json;
using Xunit;

namespace FastTests.Issues
{
    public class RavenDB_13853 : RavenTestBase
    {
        [Fact]
        public void Paging_limits_property_should_work()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 10_000; i++)
                    {
                        bulk.Store(new User { Name = "foo" + i });
                    }
                }

                using (var session = store.OpenSession())
                {
                    var operations = (InMemoryDocumentSessionOperations)session;
                    var queryCommand = new QueryCommand(operations, new IndexQuery
                    {
                        Query = "from Users limit 5 offset 10",
                    });
                    
                    using (var ctx = JsonOperationContext.ShortTermSingleUse())
                    {
                        operations.RequestExecutor.Execute(queryCommand,ctx,operations.SessionInfo);
                        Assert.Equal(5, queryCommand.Result.Results.Length);
                        Assert.Equal(10,queryCommand.Result.SkippedResults); 
                    }
                }
            }
        }
    }
}
