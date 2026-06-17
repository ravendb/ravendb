using System.Linq;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client;

public class MoreLikeThisTokenAliasTests(ITestOutputHelper output) : RavenTestBase(output)
{
    private class Article
    {
        public string Id { get; set; }
        public string Body { get; set; }
    }

    private class ArticleResult
    {
        public string Id { get; set; }
        public string Body { get; set; }
        public object Meta { get; set; }
    }

    private class ArticleIndex : AbstractIndexCreationTask<Article>
    {
        public ArticleIndex()
        {
            Map = articles => from a in articles select new { a.Body };
            Indexes.Add(a => a.Body, FieldIndexing.Search);
            TermVectors.Add(a => a.Body, FieldTermVector.Yes);
        }
    }

    [RavenFact(RavenTestCategory.Querying)]
    public void MoreLikeThisTokenSurvivesFromAliasWhenProjectionIsJsObject()
    {
        using var store = GetDocumentStore();

        using (var session = store.OpenSession())
        {
            session.Store(new Article { Body = "test test test" });
            session.Store(new Article { Body = "cake is great" });
            session.SaveChanges();
        }

        new ArticleIndex().Execute(store);
        Indexes.WaitForIndexing(store);

        using var session2 = store.OpenSession();

        var query = session2.Query<Article, ArticleIndex>()
            .MoreLikeThis(f => f.UsingDocument(@"{""Body"":""test""}").WithOptions(new MoreLikeThisOptions
            {
                MinimumTermFrequency = 1,
                MinimumDocumentFrequency = 1,
                MinimumWordLength = 0
            }))
            .Select(d => new ArticleResult
            {
                Id = d.Id,
                Body = d.Body,
                Meta = RavenQuery.Metadata(d)[Constants.Documents.Metadata.LastModified]
            });

        string rql = query.ToString();

        Assert.Contains("moreLikeThis(", rql);

        var results = query.ToList();

        Assert.Single(results);
        Assert.Equal("test test test", results[0].Body);
        Assert.NotNull(results[0].Meta);
    }
}
