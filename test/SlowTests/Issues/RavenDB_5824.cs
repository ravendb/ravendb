using System;
using System.Linq;
using System.Net.Http;
using FastTests;
using JetBrains.Annotations;
using Orders;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Http;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_5824 : RavenTestBase
    {
        [Fact]
        public void IndexProgressShouldReturnStalenessReasons()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new CreateSampleDataOperation());

                new Index().Execute(store);

                WaitForIndexing(store);

                var progress = store.Admin.Send(new GetIndexProgressOperation(new Index().IndexName));
                Assert.False(progress.IsStale);
                Assert.Empty(progress.StalenessReasons);

                store.Admin.Send(new StopIndexingOperation());

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>("orders/1");
                    var company = session.Load<Company>("companies/1");
                    var product = session.Load<Product>("products/1");
                    var supplier = session.Load<Supplier>("suppliers/1");

                    order.Freight = 10;
                    company.ExternalId = "1234";
                    product.PricePerUnit = 10;
                    supplier.Fax = "1234";

                    session.SaveChanges();
                }

                progress = store.Admin.Send(new GetIndexProgressOperation(new Index().IndexName));
                Assert.True(progress.IsStale);
                Assert.Equal(4, progress.StalenessReasons.Count);
            }
        }

        private class Index : AbstractMultiMapIndexCreationTask<Index.Result>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public Index()
            {
                AddMap<Order>(orders =>
                    from o in orders
                    let c = LoadDocument<Company>(o.Company)
                    select new
                    {
                        Name = c.Name
                    });

                AddMap<Product>(products =>
                    from p in products
                    let s = LoadDocument<Supplier>(p.Supplier)
                    select new
                    {
                        Name = s.Name
                    });
            }
        }

        private class GetIndexProgressOperation : IAdminOperation<IndexProgress>
        {
            private readonly string _indexName;

            public GetIndexProgressOperation([NotNull] string indexName)
            {
                _indexName = indexName ?? throw new ArgumentNullException(nameof(indexName));
            }

            public RavenCommand<IndexProgress> GetCommand(DocumentConventions conventions, JsonOperationContext context)
            {
                return new GetIndexProgressCommand(conventions, _indexName);
            }

            private class GetIndexProgressCommand : RavenCommand<IndexProgress>
            {
                private readonly DocumentConventions _conventions;
                private readonly string _indexName;

                public GetIndexProgressCommand(DocumentConventions conventions, string indexName)
                {
                    _conventions = conventions;
                    _indexName = indexName;
                }

                public override bool IsReadRequest => true;

                public override HttpRequestMessage CreateRequest(JsonOperationContext ctx, ServerNode node, out string url)
                {
                    url = $"{node.Url}/databases/{node.Database}/indexes/progress?name={_indexName}";

                    return new HttpRequestMessage
                    {
                        Method = HttpMethod.Get
                    };
                }

                public override void SetResponse(BlittableJsonReaderObject response, bool fromCache)
                {
                    Result = (IndexProgress)_conventions.DeserializeEntityFromBlittable(typeof(IndexProgress), response);
                }
            }
        }
    }
}
