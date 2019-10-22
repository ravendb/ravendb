using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Georgiosd : RavenTestBase
    {
        public Georgiosd(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanGet304FromLazyFacets()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new OrgIndex());

                using (var session = store.OpenSession())
                {
                    var sector1 = new Sector { Id = 1, Name = "sector1" };
                    var sector2 = new Sector { Id = 2, Name = "sector2" };

                    for (int i = 0; i < 10; ++i)
                    {
                        session.Store(new Org { Id = (i + 1).ToString(), Sectors = new List<Sector> { i % 2 == 0 ? sector1 : sector2 } });
                    }

                    session.Store(new LocalFacet());
                    session.SaveChanges();

                    session.Query<Org, OrgIndex>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .ToList();


                    for (int i = 0; i < 5; i++)
                    {
                        var facetResults = session.Query<Org, OrgIndex>().Customize(c => c.WaitForNonStaleResults())
                            .AggregateUsing(LocalFacet.Reference).ExecuteLazy().Value;
                        var facetResult = facetResults["Sectors_Id"];
                        Assert.Equal(2, facetResult.Values.Count);

                        Assert.Equal("1", facetResult.Values[0].Range);
                        Assert.Equal("2", facetResult.Values[1].Range);

                        Assert.Equal(5, facetResult.Values[0].Count);
                        Assert.Equal(5, facetResult.Values[1].Count);
                    }
                }
            }
        }
        private class Sector
        {
            public int Id { get; set; }
            public string Name { get; set; }
        }

        private class Org
        {
            public string Id { get; set; }
            public IList<Sector> Sectors { get; set; }
        }

        private class OrgIndex : AbstractIndexCreationTask<Org>
        {
            public OrgIndex()
            {
                Map = orgs => orgs.Select(org => new
                {
                    Sectors_Id = org.Sectors.Select(s => s.Id)
                });
            }
        }

        private class LocalFacet : FacetSetup
        {
            public const string Reference = "facets/TestFacet";

            public LocalFacet()
            {
                Id = Reference;
                Facets = new List<Facet>
                {
                    new Facet {FieldName = "Sectors_Id"}
                };
            }
        }
    }
}
