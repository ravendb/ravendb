using System;
using FastTests;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class MultiMapIndexQueryTests : RavenTestBase
    {
        public MultiMapIndexQueryTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanQuery()
        {
            using (var store = GetDocumentStore())
            {
                await Setup(store);
                using (var session = store.OpenAsyncSession())
                {
                    //ask
                    var query = await session.Query<ArticleInfo, StockInfoIndex>()
                        .Where(x => x.Total > 0 && x.Delta < 0)
                        .Select(x => new
                        {
                            x.Id,
                            x.Quantity,
                            x.Title,
                            x.InStock,
                            x.Total
                        }).ToListAsync();

                    Assert.True(query.Any());
                    Assert.Equal(6, query.Count);
                }
            }
        }

        [Fact] 
        public async Task ShouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                await Setup(store);
                using (var session = store.OpenAsyncSession())
                {
                    //ask
                    var query = session.Query<ArticleInfo, StockInfoIndex>()
                        .Where(x => x.Total > 0 && x.Total < x.InStock)
                        .Select(x => new
                        {
                            x.Id,
                            x.Quantity,
                            x.Title,
                            x.InStock,
                            x.Total
                        });

                    await Assert.ThrowsAsync<NotSupportedException>(async () => await query.ToListAsync());
                }
            }
        }

        private async Task Setup(DocumentStore store)
        {
            await new StockInfoIndex().ExecuteAsync(store);

            using (var session = store.OpenAsyncSession())
            {
                //arrange    
                foreach (var i in Enumerable.Range(1, 100))
                {
                    var article = new Article
                    {
                        Id = $"articles/000{i}",
                        Title = "BLBLBL",
                        Quantity = 1
                    };
                    await session.StoreAsync(article);
                }

                foreach (var i in Enumerable.Range(1, 10))
                {
                    await session.StoreAsync(new StockInfo()
                    {
                        ArticleId = $"articles/000{i}",
                        Total = 100,
                        InStock = i < 5 ? 25 : 125
                    });
                }

                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);
        }

        private class Article
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public int? Quantity { get; set; }
        }

        private class StockInfo
        {
            public string Id { get; set; }
            public string ArticleId { get; set; }
            public int? Total { get; set; }
            public int? InStock { get; set; }
        }

        private class ArticleInfo
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public int? Quantity { get; set; }
            public int? Total { get; set; }
            public int? InStock { get; set; }
            public int? Delta { get; set; }
        }


        private class StockInfoIndex : AbstractMultiMapIndexCreationTask<ArticleInfo>
        {
            public StockInfoIndex()
            {
                AddMap<StockInfo>(infos => from c in infos
                                           let article = LoadDocument<Article>(c.ArticleId)
                                           select new
                                           {
                                               Id = c.ArticleId,
                                               Title = article.Title,
                                               Quantity = article.Quantity,
                                               Total = c.Total,
                                               InStock = c.InStock,
                                               Delta = c.Total - c.InStock
                                           });

                AddMap<Article>(articles => from article in articles
                                            select new
                                            {
                                                Id = article.Id,
                                                Title = article.Title,
                                                Quantity = (int?)null,
                                                Total = (int?)null,
                                                InStock = (int?)null,
                                                Delta = (int?)null
                                            });

                Reduce = results => from result in results
                                    group result by result.Id into g
                                    where g.Key != null
                                    select new
                                    {
                                        Id = g.Key,
                                        Title = g.Select(x => x.Title).FirstOrDefault(),
                                        Quantity = g.Select(x => x.Quantity).FirstOrDefault(),
                                        Total = g.Where(x => x.Total != null).Select(x => x.Total).FirstOrDefault(),
                                        InStock = g.Where(x => x.InStock != null).Select(x => x.InStock).FirstOrDefault(),
                                        Delta = g.Select(x => x.Total - x.InStock).FirstOrDefault()
                                    };

                Store(x => x.Id, FieldStorage.Yes);
                Store(x => x.Title, FieldStorage.Yes);
                Store(x => x.Total, FieldStorage.Yes);
                Store(x => x.Quantity, FieldStorage.Yes);
                Store(x => x.InStock, FieldStorage.Yes);
                Store(x => x.Delta, FieldStorage.Yes);
            }
        }
    }
}
