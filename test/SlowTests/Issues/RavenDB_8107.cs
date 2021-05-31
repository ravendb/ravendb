using System;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_8107 : RavenTestBase
    {
        public RavenDB_8107(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Can_patch_by_dynamic_collection_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order(), "orders/1");
                    
                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation(
                        new IndexQuery { Query = "FROM Orders UPDATE { this.Company = 'HR'; } " }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");

                    Assert.Equal("HR", order.Company);
                }
            }
        }

        [Fact]
        public void Can_delete_by_dynamic_collection_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order(), "orders/1");

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new DeleteByQueryOperation(
                    new IndexQuery { Query = "FROM Orders" }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Order>("orders/1"));
                }
            }
        }

        [Fact]
        public void Can_patch_by_dynamic_all_docs_collection_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order(), "orders/1");

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new PatchByQueryOperation(
                    new IndexQuery { Query = "FROM @all_docs UPDATE { this.Company = 'HR';} " }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");

                    Assert.Equal("HR", order.Company);
                }
            }
        }

        [Fact]
        public void Can_delete_by_dynamic_all_docs_collection_query()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order());

                    session.SaveChanges();
                }

                var operation = store.Operations.Send(new DeleteByQueryOperation(
                    new IndexQuery { Query = "FROM @all_docs" }));

                operation.WaitForCompletion(TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    Assert.Null(session.Load<Order>("orders/1-A"));
                }

                var statistics = store.Maintenance.Send(new GetStatisticsOperation());

                Assert.Equal(1, statistics.CountOfDocuments); // hilo doc
            }
        }
    }
}
