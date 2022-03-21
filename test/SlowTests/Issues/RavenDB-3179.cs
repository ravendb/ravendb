using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3179 : RavenTestBase
    {
        public RavenDB_3179(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task To_Facet_Lazy_Async()
        {
            using (var store = GetDocumentStore())
            {
                new AdviceSearch().Execute(store);
                using (var session = store.OpenSession())
                {
                    SetupFacets(session);
                    GenerateData(session);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    QueryStatistics stats;
                    var query = session.Query<AdviceSearch.Result, AdviceSearch>()
                        .Where(x => x.Sections == "Sections/1")
                        .Search(x => x.SearchField, "How to", options: SearchOptions.And)
                        .Include(x => x.Sections);


                    var facetResultsLazy = query.AggregateUsing("facets/ArticleFacets").ExecuteLazyAsync();

                    var articleResults = query
                        .Statistics(out stats)
                        .OrderByScoreDescending()
                        .As<Article>()
                        .LazilyAsync();


                    await session.Advanced.Eagerly.ExecuteAllPendingLazyOperationsAsync();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);


                    var facetResults = await facetResultsLazy.Value;


                    var sections = facetResults.FirstOrDefault().Value;

                    var results = sections.Values.Select(async (value) =>
                   {
                       // Doesn't contact server due to Include in original query
                       var section = await session.LoadAsync<Section>(value.Range);
                       return new SectionFacet
                       {
                           Id = section.Id,
                           Name = section.Name,
                           Count = value.Count
                       };
                   }).ToList();
                    await Task.WhenAll(results);

                    IList<SectionFacet> listSections = results.Select(x => x.Result).ToList();
                }
            }
        }

        private class Article
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public string Description { get; set; }
            public string Content { get; set; }
            // List of section IDs the article is listed under
            public string[] Sections { get; set; }
        }

        private class Section
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Slug { get; set; }
        }
        private class AdviceSearch : AbstractIndexCreationTask<Article, AdviceSearch.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string SearchField { get; set; }
                public string Sections { get; set; }
            }

            public AdviceSearch()
            {
                Map = articles =>
                      from article in articles
                      select new
                      {
                          article.Id,
                          SearchField = new object[]
                          {
                              article.Title,
                              article.Description,
                              article.Content
                          },
                          article.Sections
                      };

                Index(x => x.SearchField, FieldIndexing.Search);
            }
        }

        private class SectionFacet
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Count { get; set; }
        }

        private static void SetupFacets(IDocumentSession session)
        {
            session.Store(new FacetSetup
            {
                Id = "facets/ArticleFacets",
                Facets = new[] { new Facet { FieldName = "Sections" } }.ToList()
            });
        }

        private static void GenerateData(IDocumentSession session)
        {
            var sections = new[]
            {
                new Section {Id = "Sections/1", Name = "TV Articles", Slug = "tv-articles"},
                new Section {Id = "Sections/2", Name = "General Articles", Slug = "general"}
            };
            var articles = new[]
            {
                new Article {Id = "Articles/1", Title = "How to fix your TV", Description = "How to", Sections = new[] {"Sections/1", "Sections/2"}},
                new Article {Id = "Articles/2", Title = "How to do something", Description = "How to", Sections = new[] {"Sections/2"}}
            };
            foreach (var section in sections) session.Store(section);
            foreach (var article in articles) session.Store(article);
        }
    }
}
