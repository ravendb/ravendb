using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Core.Streaming
{
    public class OnBeforeQuery : RavenTestBase
    {
        public OnBeforeQuery(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldStreamArticlesOfTenant1()
        {
            const string tenantA = "tenantA";
            const string tenantB = "tenantB";

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession(store.Database))
                {
                    await session.StoreAsync(new Article(tenantA, "Article1", false));
                    await session.StoreAsync(new Article(tenantA, "Article2", false));
                    await session.StoreAsync(new Article(tenantA, "Article Deleted", true));

                    await session.StoreAsync(new Article(tenantB, "Article3", false));
                    await session.StoreAsync(new Article(tenantB, "Article4", false));
                    await session.StoreAsync(new Article(tenantB, "Article5 Deleted", true));

                    await session.SaveChangesAsync();

                    session.Advanced.OnBeforeQuery += (sender, args) =>
                    {
                        dynamic queryToBeExecuted = args.QueryCustomization;
                        queryToBeExecuted.Intersect();
                        queryToBeExecuted.WhereEquals(nameof(Article.TenantId), tenantA);
                    };

                    IRavenQueryable<Article> query =
                        session.Query<Article>().Where(article => article.Deleted == false);

                    List<Article> queryResult = await query.ToListAsync();
                    Console.WriteLine(queryResult.Count);
                    Assert.Equal(2, queryResult.Count);
                    Assert.True(queryResult.All(x => x.TenantId.Equals(tenantA, StringComparison.Ordinal)));

                    IAsyncEnumerator<StreamResult<Article>> streamResult = await session.Advanced.StreamAsync(query);
                    var streamedItems = new List<Article>();
                    while (await streamResult.MoveNextAsync())
                    {
                        streamedItems.Add(streamResult.Current.Document);
                    }

                    Assert.Equal(2, streamedItems.Count);
                    Assert.True(streamedItems.All(x => x.TenantId.Equals(tenantA, StringComparison.Ordinal)));
                }
            }
        }


        public class Article
        {
            public Article(string tenantId, string title, bool deleted)
            {
                TenantId = tenantId;
                Title = title;
                Deleted = deleted;
            }

            public string Id { get; private set; } = string.Empty;

            public string TenantId { get; private set; }

            public string Title { get; private set; }

            public bool Deleted { get; private set; }
        }
    }
}
