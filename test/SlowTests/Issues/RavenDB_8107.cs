using System;
using FastTests;
using FastTests.Server.Basic.Entities;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Exceptions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8107 : RavenTestBase
    {
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
        public void Patch_and_delete_by_dynamic_collection_query_with_filtering_should_throw()
        {
            using (var store = GetDocumentStore())
            {
                var ex = Assert.Throws<BadRequestException>(() => store.Operations.Send(new PatchByQueryOperation(
                    new IndexQuery { Query = "FROM Orders WHERE Company = 'companies/1' UPDATE { this.Company = 'HR'; } " })));

                Assert.Contains("Patch and delete documents by a dynamic query is supported only for queries having just FROM clause and" +
                                " optionally simple WHERE filtering using '=' or 'IN' operators on document identifiers," +
                                " e.g. FROM Orders, FROM Orders WHERE id() = 'orders/1', FROM Orders WHERE id() IN ('orders/1', 'orders/2'). " +
                                "If you need to perform different filtering please issue the query to the static index", ex.Message);

                ex = Assert.Throws<BadRequestException>(() => store.Operations.Send(new DeleteByQueryOperation(
                    new IndexQuery { Query = "FROM Orders WHERE Company = 'companies/1'" })));

                Assert.Contains("Patch and delete documents by a dynamic query is supported only for queries having just FROM clause and" +
                                " optionally simple WHERE filtering using '=' or 'IN' operators on document identifiers," +
                                " e.g. FROM Orders, FROM Orders WHERE id() = 'orders/1', FROM Orders WHERE id() IN ('orders/1', 'orders/2'). " +
                                "If you need to perform different filtering please issue the query to the static index", ex.Message);
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

                var statistics = store.Admin.Send(new GetStatisticsOperation());

                Assert.Equal(1, statistics.CountOfDocuments); // hilo doc
            }
        }
    }
}
