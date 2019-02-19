// -----------------------------------------------------------------------
//  <copyright file="JustFacetSearch.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class JustFacetSearch : RavenTestBase
    {
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

        private class Advice_Search : AbstractIndexCreationTask<Article, Advice_Search.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string SearchField { get; set; }
                public string Sections { get; set; }
            }

            public Advice_Search()
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

        private class FacetSearcher
        {
            readonly IDocumentStore _store;

            public FacetSearcher(IDocumentStore store)
            {
                _store = store;
            }

            public IList<SectionFacet> FacetSearch(string searchTerm, int? sectionId = null, int page = 1, int pageSize = 10)
            {
                using (var session = _store.OpenSession())
                {
                    QueryStatistics stats;
                    var query = session.Query<Advice_Search.Result, Advice_Search>()
                        .Statistics(out stats)
                        // Optimize loading of section facets
                        .Include(x => x.Sections);

                    // Filter by section
                    if (sectionId != null)
                    {
                        var ravenId = "Sections/" + sectionId + "-A";
                        query = query.Where(x => x.Sections == ravenId);
                    }

                    query = query.Search(x => x.SearchField, searchTerm, options: SearchOptions.And);

                    var results = query
                        .Skip((page - 1) * pageSize)
                        .Take(pageSize)
                        .As<Article>();

                    // The following returns NO RESULTS unless I include: 
                    // var something = results.ToList(); // Throw away list
                    return getFacets(results, session);
                }
            }

            IList<SectionFacet> getFacets(IQueryable<Article> results, IDocumentSession session)
            {
                var facetResults = results.AggregateUsing("facets/ArticleFacets").Execute();
                var sections = facetResults.FirstOrDefault().Value;
                return sections.Values.Select(value =>
                {
                    // Doesn't contact server due to Include in original query
                    var section = session.Load<Section>(value.Range);
                    return new SectionFacet
                    {
                        Id = section.Id.ToString(),
                        Name = section.Name,
                        Count = value.Count
                    };
                }).ToList();
            }
        }

        private void setupFacets(IDocumentSession session)
        {
            session.Store(new FacetSetup
            {
                Id = "facets/ArticleFacets",
                Facets = new[] { new Facet { FieldName = "Sections" } }.ToList()
            });
        }

        private void generateData(IDocumentSession session)
        {
            var sections = new[]
            {
                new Section {Name = "TV Articles", Slug = "tv-articles"},
                new Section {Name = "General Articles", Slug = "general"}
            };
            var articles = new[]
            {
                new Article {Title = "How to fix your TV", Description = "How to", Sections = new[] {"Sections/1-A", "Sections/2-A"}},
                new Article {Title = "How to do something", Description = "How to", Sections = new[] {"Sections/2-A"}}
            };
            foreach (var section in sections) session.Store(section);
            foreach (var article in articles) session.Store(article);
        }

        [Fact]
        public void JustReturnFacets()
        {
            using (var store = GetDocumentStore())
            {
                new Advice_Search().Execute(store);

                var searcher = new FacetSearcher(store);
                using (var session = store.OpenSession())
                {
                    setupFacets(session);
                    generateData(session);
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                var facets = searcher.FacetSearch("how to");
                Assert.Equal(2, facets.Count);
                Assert.Equal("TV Articles", facets[0].Name);
                Assert.Equal("General Articles", facets[1].Name);
            }
        }
    }
}
