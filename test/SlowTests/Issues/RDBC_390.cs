using FastTests;
using Orders;
using Raven.Client.Documents.Queries.Facets;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_390 : RavenTestBase
    {
        public RDBC_390(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void FacetQueryShouldEscapeFieldNameAndDisplayFieldNameProperlyIfNeeded()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var facetQuery = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .AggregateBy(new[]
                        {
                            new Facet
                            {
                                FieldName = "Colour with space",
                                DisplayFieldName = "Alias with space"
                            },
                            new Facet
                            { 
                                FieldName = "Ability" 
                            }
                        });

                    var query = facetQuery.ToString();

                    Assert.Equal("from 'Companies' select facet('Colour with space') as 'Alias with space', facet(Ability)", query);
                }
            }
        }
    }
}
