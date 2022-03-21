using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15468 : RavenTestBase
    {
        public RavenDB_15468(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
            {
                new Index_Projects_Search().Execute(store);

                var siteId = "site/322";
                var portfolioId1 = $"{siteId}/portfolio";
                var portfolioId2 = "portfolio/228";
                var portfolioKey = portfolioId1.Replace("/", "-");

                var projectIds1 = new List<string>();
                var projectIds2 = new List<string>();
                for (int i = 0; i < 5; i++)
                {
                    var id = $"projects/{i}";
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Project()
                        {
                            SiteId = siteId,
                            Portfolios = new List<string>()
                            {
                                portfolioId1,
                                portfolioId2
                            }
                        }, id);
                        session.SaveChanges();
                    }

                    projectIds1.Add(id);
                    projectIds2.Add(id);
                }
                for (int i = 5; i < 10; i++)
                {
                    var id = $"projects/{i}";
                    using (var session = store.OpenSession())
                    {
                        session.Store(new Project()
                        {
                            SiteId = siteId,
                            Portfolios = new List<string>()
                            {
                                portfolioId1
                            }
                        }, id);
                        session.SaveChanges();
                    }
                    projectIds1.Add(id);
                }

                var normalList = new List<string>(projectIds1);
                projectIds1.Reverse();
                projectIds2.Reverse();

                using (var session = store.OpenSession())
                {
                    session.Store(new Portfolio()
                    {
                        SiteId = siteId,
                        Projects = projectIds1
                    }, portfolioId1);
                    session.Store(new Portfolio()
                    {
                        SiteId = siteId,
                        Projects = projectIds2
                    }, portfolioId2);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    // from index 'Projects/Search' where SiteId = "site/322" order by site-322-portfolio as long
                    var query = session.Advanced.DocumentQuery<Project>("Projects/Search")
                        .WhereEquals(x => x.SiteId, siteId)
                        .AddOrder(portfolioKey, false, OrderingType.Long);

                    WaitForUserToContinueTheTest(store);

                    var orderedIdsList = query.ToList().Select(x => x.Id).ToList();
                    var portfoliosOrder = session.Load<Portfolio>(portfolioId1);
                    Assert.Equal(portfoliosOrder.Projects.Count, orderedIdsList.Count);

                    var index = 0;
                    foreach (var id in orderedIdsList)
                    {
                        var orderedId = portfoliosOrder.Projects.ElementAt(index);
                        Assert.Equal(id, orderedId);
                        index++;
                    }

                    portfoliosOrder.Projects = normalList;
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.DocumentQuery<Project>("Projects/Search")
                        .WaitForNonStaleResults()
                        .WhereEquals(x => x.SiteId, siteId)
                        .AddOrder(portfolioKey, false, OrderingType.Long);

                    var orderedIdsList = query.ToList().Select(x => x.Id).ToList();
                    var portfoliosOrder = session.Load<Portfolio>(portfolioId1);
                    Assert.Equal(portfoliosOrder.Projects.Count, orderedIdsList.Count);

                    var index = 0;
                    foreach (var id in orderedIdsList)
                    {
                        var orderedId = portfoliosOrder.Projects.ElementAt(index);
                        Assert.Equal(id, orderedId);
                        index++;
                    }
                }
            }
        }

        private class Index_Projects_Search : AbstractIndexCreationTask
        {
            public override string IndexName => "Projects/Search";

            public override IndexDefinition CreateIndexDefinition()
            {
                return new IndexDefinition
                {
                    Maps =
                    {
                        @"docs.Projects.Select(project => new {
    project = project,
    portfolios = this.LoadDocument(project.Portfolios, ""Portfolios"").Where(p => p != null)
}).Select(this0 => new {
    this0 = this0,
    mappings = this0.portfolios.GroupBy(p0 => p0.Id)
}).Select(this1 => new {
    Id = Id(this1.this0.project),
    SiteId = this1.this0.project.SiteId,
    _ = this1.mappings.Select(mapping => this.CreateField(mapping.Key.Replace(""/"", ""-""), mapping.Select(x => x.Projects.IndexOf(Id(this1.this0.project)))))
})"
                    },
                    Fields =
                    {
                        { "Terms", new IndexFieldOptions
                        {
                            Indexing = FieldIndexing.Search } }
                    }
                };
            }
        }

        private class Project
        {
            public List<string> Portfolios { get; set; }
            public string SiteId { get; set; }
            public string Id { get; set; }
        }

        private class Portfolio
        {
            public List<string> Projects { get; set; }
            public List<string> Portfolios { get; set; }
            public string SiteId { get; set; }
            public string Id { get; set; }
        }
    }
}
