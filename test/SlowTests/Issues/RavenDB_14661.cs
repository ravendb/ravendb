using System;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;
namespace SlowTests.Issues
{
    public class RavenDB_14661 : RavenTestBase
    {
        public RavenDB_14661(ITestOutputHelper output) : base(output)
        {
        }
        
        [Fact]
        public void PatchShouldThrowIfAllowStaleIsSetToFalseAndTimeoutHasPassed()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Companies_ByName();
                index.Execute(store);
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Name1" });
                    session.SaveChanges();
                }
                
                Indexes.WaitForIndexing(store);
                
                var iq = new IndexQuery
                {
                    Query = $"from index '{index.IndexName}' as c update {{ c.Name = 'Name2' }}"
                };
                
                var operation = store.Operations.Send(new PatchByQueryOperation(iq, new QueryOperationOptions
                {
                    AllowStale = true
                }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));
                
                Indexes.WaitForIndexing(store);
                
                operation = store.Operations.Send(new PatchByQueryOperation(iq, new QueryOperationOptions
                {
                    AllowStale = false
                }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));
                
                store.Maintenance.Send(new StopIndexingOperation());
                
                using (var session = store.OpenSession())
                {
                    session.Store(new Company { Name = "Name3" });
                    session.SaveChanges();
                }
                
                operation = store.Operations.Send(new PatchByQueryOperation(iq, new QueryOperationOptions
                {
                    AllowStale = true
                }));
                operation.WaitForCompletion(TimeSpan.FromSeconds(30));
                
                operation = store.Operations.Send(new PatchByQueryOperation(iq, new QueryOperationOptions
                {
                    AllowStale = false,
                    StaleTimeout = TimeSpan.FromMilliseconds(1)
                }));
                
                var e = Assert.Throws<RavenException>(() => operation.WaitForCompletion(TimeSpan.FromSeconds(30)));
                Assert.Contains("Cannot perform bulk operation. Index is stale.", e.Message);
            }
        }
        
        private class Companies_ByName : AbstractIndexCreationTask<Company>
        {
            public Companies_ByName()
            {
                Map = companies => from c in companies 
                    select new
                    {
                        c.Name
                    };
            }
        }
    }
}
