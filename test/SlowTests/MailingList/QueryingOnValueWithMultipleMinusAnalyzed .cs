using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents.Indexes.Persistence.Lucene.Analyzers;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class QueryingOnValueWithMultipleMinusAnalyzed : RavenTestBase
    {
        public QueryingOnValueWithMultipleMinusAnalyzed(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanPerformQueryWithDashesInTerm()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinitionBuilder<Product>()
                {
                    Map = products => from product in products
                                      select new
                                      {
                                          Query = new object[]
                                          {
                                                product.ItemNumber,
                                                product.ItemDescription,

                                          },
                                          product.ProductId

                                      },
                    Indexes =
                    {
                        {x => x.Query, FieldIndexing.Search}
                    },
                    Analyzers =
                    {
                        {x => x.Query, typeof (LowerCaseWhitespaceAnalyzer).AssemblyQualifiedName}
                    }

                }.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "someIndex";
                store.Maintenance.Send(new PutIndexesOperation(new[] {indexDefinition}));


                var prodOne = new Product
                {
                    ProductId = "one",
                    ItemNumber = "Q9HT180-Z-Q",
                    ItemDescription = "PILLOW PROTECTOR QUEEN"
                };
                var prodTwo = new Product
                {
                    ProductId = "two",
                    ItemNumber = "Q9HT180-Z-U",
                    ItemDescription = "PILLOW PROTECTOR STANDARD"
                };
                var prodThree = new Product
                {
                    ProductId = "three",
                    ItemNumber = "Q9HT180-Z-K",
                    ItemDescription = "PILLOW PROTECTOR KING"
                };

                using (var session = store.OpenSession())
                {
                    session.Store(prodOne);
                    session.Store(prodTwo);
                    session.Store(prodThree);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var prods = session.Advanced.DocumentQuery<Product>("someIndex")
                        .WaitForNonStaleResults()
                        .WhereStartsWith(x => x.Query, "Q9HT180-Z-K")
                        .ToList();

                    Assert.Equal(1, prods.Count);
                }
            }
        }

        private class Product
        {
            public string ProductId { get; set; }

            public string Query { get; set; }

            public string ItemNumber { get; set; }

            public string ItemDescription { get; set; }

        }
    }
}
