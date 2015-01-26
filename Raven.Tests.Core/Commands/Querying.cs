using System.Diagnostics;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Json.Linq;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Core.Commands
{
    public class Querying : RavenCoreTestBase
    {
        [Fact]
        public void CanDoSimpleQueryOnDatabase()
        {
            const string indexName = "CompaniesByName";
            using (var store = GetDocumentStore())
            {
                var contact1 = new Contact { FirstName = "Expression Name" };
                var contact2 = new Contact { FirstName = "Expression First Name" };
                var contact3 = new Contact { FirstName = "First Name" };

                store.DatabaseCommands.Put("contacts/1", null, RavenJObject.FromObject(contact1), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });
                store.DatabaseCommands.Put("contacts/2", null, RavenJObject.FromObject(contact2), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });
                store.DatabaseCommands.Put("contacts/3", null, RavenJObject.FromObject(contact3), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });

                store.DatabaseCommands.PutIndex(indexName, new IndexDefinition()
				{
					Map = "from contact in docs.Contacts select new { contact.FirstName }"
				}, false);
                WaitForIndexing(store);

                var companies = store.DatabaseCommands.Query(indexName, new IndexQuery { Query = "" }, null);
                Assert.Equal(3, companies.TotalResults);
                Assert.Equal("Expression Name", companies.Results[0].Value<string>("FirstName"));
                Assert.Equal("Expression First Name", companies.Results[1].Value<string>("FirstName"));
                Assert.Equal("First Name", companies.Results[2].Value<string>("FirstName"));
            }
        }

        [Fact]
        public void CanStreamQueryResult()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                for (int i = 0; i < 30; i++)
                {
                    store.DatabaseCommands.Put("users/" + i, null, RavenJObject.FromObject(new User { Name = "Name"+i }), new RavenJObject { { "Raven-Entity-Name", "Users" } });
                }
                WaitForIndexing(store);

                int count = 0;
                QueryHeaderInformation queryHeaders = null;
                var reader = store.DatabaseCommands.StreamQuery(index.IndexName, new IndexQuery { Query = "" }, out queryHeaders);
                while (reader.MoveNext())
                {
                    Assert.Equal("Name" + count, reader.Current.Value<string>("Name"));
                    count++;
                }
                Assert.Equal(30, count);
            }
        }

        [Fact]
        public void CanGetFacets()
        {
            using (var store = GetDocumentStore())
            {
                var index = new CameraCost();
                index.Execute(store);

                for (int i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.Put(
                        "cameras/"+i, 
                        null, 
                        RavenJObject.FromObject(new Camera
                        {
                            Id = "cameras/" + i,
                            Manufacturer = i % 2 == 0 ? "Manufacturer1" : "Manufacturer2",
                            Cost = i * 100D,
                            Megapixels = i * 1D
                        }),
                        new RavenJObject { { "Raven-Entity-Name", "Cameras" } });
                }
                WaitForIndexing(store);

                var facets = new List<Facet>
                {
                    new Facet 
                    {
                        Name = "Manufacturer"
                    },
                    new Facet 
                    {
                        Name = "Cost_Range",
                        Mode = FacetMode.Ranges,
                        Ranges = 
                        {
                            "[NULL TO Dx200.0]",
                            "[Dx300.0 TO Dx400.0]",
                            "[Dx500.0 TO Dx600.0]",
                            "[Dx700.0 TO Dx800.0]",
                            "[Dx900.0 TO NULL]"
                        }
                    },
                    new Facet 
                    {
                        Name = "Megapixels_Range",
                        Mode = FacetMode.Ranges,
                        Ranges = 
                        {
                            "[NULL TO Dx3.0]",
                            "[Dx4.0 TO Dx7.0]",
                            "[Dx8.0 TO Dx10.0]",
                            "[Dx11.0 TO NULL]"
                        }
                    }
                };
                store.DatabaseCommands.Put(
                    "facets/CameraFacets", 
                    null, 
                    RavenJObject.FromObject(new FacetSetup { Id = "facets/CameraFacets", Facets = facets }), 
                    new RavenJObject());
                WaitForIndexing(store);

                var facetResults = store.DatabaseCommands.GetFacets(index.IndexName, new IndexQuery{Query=""}, "facets/CameraFacets");

                Assert.Equal(3, facetResults.Results.Count);

                Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count);
                Assert.Equal("manufacturer1", facetResults.Results["Manufacturer"].Values[0].Range);
                Assert.Equal(5, facetResults.Results["Manufacturer"].Values[0].Hits);
                Assert.Equal("manufacturer2", facetResults.Results["Manufacturer"].Values[1].Range);
                Assert.Equal(5, facetResults.Results["Manufacturer"].Values[1].Hits);

                Assert.Equal(5, facetResults.Results["Cost_Range"].Values.Count);
                Assert.Equal("[NULL TO Dx200.0]", facetResults.Results["Cost_Range"].Values[0].Range);
                Assert.Equal(3, facetResults.Results["Cost_Range"].Values[0].Hits);
                Assert.Equal("[Dx300.0 TO Dx400.0]", facetResults.Results["Cost_Range"].Values[1].Range);
                Assert.Equal(2, facetResults.Results["Cost_Range"].Values[1].Hits);
                Assert.Equal("[Dx500.0 TO Dx600.0]", facetResults.Results["Cost_Range"].Values[2].Range);
                Assert.Equal(2, facetResults.Results["Cost_Range"].Values[2].Hits);
                Assert.Equal("[Dx700.0 TO Dx800.0]", facetResults.Results["Cost_Range"].Values[3].Range);
                Assert.Equal(2, facetResults.Results["Cost_Range"].Values[3].Hits);
                Assert.Equal("[Dx900.0 TO NULL]", facetResults.Results["Cost_Range"].Values[4].Range);
                Assert.Equal(1, facetResults.Results["Cost_Range"].Values[4].Hits);

                Assert.Equal(4, facetResults.Results["Megapixels_Range"].Values.Count);
                Assert.Equal("[NULL TO Dx3.0]", facetResults.Results["Megapixels_Range"].Values[0].Range);
                Assert.Equal(4, facetResults.Results["Megapixels_Range"].Values[0].Hits);
                Assert.Equal("[Dx4.0 TO Dx7.0]", facetResults.Results["Megapixels_Range"].Values[1].Range);
                Assert.Equal(4, facetResults.Results["Megapixels_Range"].Values[1].Hits);
                Assert.Equal("[Dx8.0 TO Dx10.0]", facetResults.Results["Megapixels_Range"].Values[2].Range);
                Assert.Equal(2, facetResults.Results["Megapixels_Range"].Values[2].Hits);
                Assert.Equal("[Dx11.0 TO NULL]", facetResults.Results["Megapixels_Range"].Values[3].Range);
                Assert.Equal(0, facetResults.Results["Megapixels_Range"].Values[3].Hits);


                var multiFacetResults = store.DatabaseCommands.GetMultiFacets(new FacetQuery[] 
                {
                    new FacetQuery 
                    { 
                        IndexName = index.IndexName, 
                        Query = new IndexQuery 
                        { 
                            Query = "Cost:{NULL TO 200}" 
                        }, 
                        FacetSetupDoc = "facets/CameraFacets" 
                    },
                    new FacetQuery 
                    { 
                        IndexName = index.IndexName, 
                        Query = new IndexQuery 
                        { 
                            Query = "Megapixels:{NULL TO 3}" 
                        }, 
                        FacetSetupDoc = "facets/CameraFacets" 
                    }
                });

                Assert.Equal(3, multiFacetResults[0].Results.Count);

                Assert.Equal(2, multiFacetResults[0].Results["Manufacturer"].Values.Count);
                Assert.Equal("manufacturer1", multiFacetResults[0].Results["Manufacturer"].Values[0].Range);
                Assert.Equal(1, multiFacetResults[0].Results["Manufacturer"].Values[0].Hits);
                Assert.Equal("manufacturer2", multiFacetResults[0].Results["Manufacturer"].Values[1].Range);
                Assert.Equal(1, multiFacetResults[0].Results["Manufacturer"].Values[1].Hits);

                Assert.Equal(5, multiFacetResults[0].Results["Cost_Range"].Values.Count);
                Assert.Equal("[NULL TO Dx200.0]", multiFacetResults[0].Results["Cost_Range"].Values[0].Range);
                Assert.Equal(2, multiFacetResults[0].Results["Cost_Range"].Values[0].Hits);
                Assert.Equal("[Dx300.0 TO Dx400.0]", multiFacetResults[0].Results["Cost_Range"].Values[1].Range);
                Assert.Equal(0, multiFacetResults[0].Results["Cost_Range"].Values[1].Hits);
                Assert.Equal("[Dx500.0 TO Dx600.0]", multiFacetResults[0].Results["Cost_Range"].Values[2].Range);
                Assert.Equal(0, multiFacetResults[0].Results["Cost_Range"].Values[2].Hits);
                Assert.Equal("[Dx700.0 TO Dx800.0]", multiFacetResults[0].Results["Cost_Range"].Values[3].Range);
                Assert.Equal(0, multiFacetResults[0].Results["Cost_Range"].Values[3].Hits);
                Assert.Equal("[Dx900.0 TO NULL]", multiFacetResults[0].Results["Cost_Range"].Values[4].Range);
                Assert.Equal(0, multiFacetResults[0].Results["Cost_Range"].Values[4].Hits);

                Assert.Equal(4, multiFacetResults[0].Results["Megapixels_Range"].Values.Count);
                Assert.Equal("[NULL TO Dx3.0]", multiFacetResults[0].Results["Megapixels_Range"].Values[0].Range);
                Assert.Equal(2, multiFacetResults[0].Results["Megapixels_Range"].Values[0].Hits);
                Assert.Equal("[Dx4.0 TO Dx7.0]", multiFacetResults[0].Results["Megapixels_Range"].Values[1].Range);
                Assert.Equal(0, multiFacetResults[0].Results["Megapixels_Range"].Values[1].Hits);
                Assert.Equal("[Dx8.0 TO Dx10.0]", multiFacetResults[0].Results["Megapixels_Range"].Values[2].Range);
                Assert.Equal(0, multiFacetResults[0].Results["Megapixels_Range"].Values[2].Hits);
                Assert.Equal("[Dx11.0 TO NULL]", multiFacetResults[0].Results["Megapixels_Range"].Values[3].Range);
                Assert.Equal(0, multiFacetResults[0].Results["Megapixels_Range"].Values[3].Hits);


                Assert.Equal(3, multiFacetResults[1].Results.Count);

                Assert.Equal(2, multiFacetResults[1].Results["Manufacturer"].Values.Count);
                Assert.Equal("manufacturer1", multiFacetResults[1].Results["Manufacturer"].Values[0].Range);
                Assert.Equal(2, multiFacetResults[1].Results["Manufacturer"].Values[0].Hits);
                Assert.Equal("manufacturer2", multiFacetResults[1].Results["Manufacturer"].Values[1].Range);
                Assert.Equal(1, multiFacetResults[1].Results["Manufacturer"].Values[1].Hits);

                Assert.Equal(5, multiFacetResults[1].Results["Cost_Range"].Values.Count);
                Assert.Equal("[NULL TO Dx200.0]", multiFacetResults[1].Results["Cost_Range"].Values[0].Range);
                Assert.Equal(3, multiFacetResults[1].Results["Cost_Range"].Values[0].Hits);
                Assert.Equal("[Dx300.0 TO Dx400.0]", multiFacetResults[1].Results["Cost_Range"].Values[1].Range);
                Assert.Equal(0, multiFacetResults[1].Results["Cost_Range"].Values[1].Hits);
                Assert.Equal("[Dx500.0 TO Dx600.0]", multiFacetResults[1].Results["Cost_Range"].Values[2].Range);
                Assert.Equal(0, multiFacetResults[1].Results["Cost_Range"].Values[2].Hits);
                Assert.Equal("[Dx700.0 TO Dx800.0]", multiFacetResults[1].Results["Cost_Range"].Values[3].Range);
                Assert.Equal(0, multiFacetResults[1].Results["Cost_Range"].Values[3].Hits);
                Assert.Equal("[Dx900.0 TO NULL]", multiFacetResults[1].Results["Cost_Range"].Values[4].Range);
                Assert.Equal(0, multiFacetResults[1].Results["Cost_Range"].Values[4].Hits);

                Assert.Equal(4, multiFacetResults[1].Results["Megapixels_Range"].Values.Count);
                Assert.Equal("[NULL TO Dx3.0]", multiFacetResults[1].Results["Megapixels_Range"].Values[0].Range);
                Assert.Equal(3, multiFacetResults[1].Results["Megapixels_Range"].Values[0].Hits);
                Assert.Equal("[Dx4.0 TO Dx7.0]", multiFacetResults[1].Results["Megapixels_Range"].Values[1].Range);
                Assert.Equal(0, multiFacetResults[1].Results["Megapixels_Range"].Values[1].Hits);
                Assert.Equal("[Dx8.0 TO Dx10.0]", multiFacetResults[1].Results["Megapixels_Range"].Values[2].Range);
                Assert.Equal(0, multiFacetResults[1].Results["Megapixels_Range"].Values[2].Hits);
                Assert.Equal("[Dx11.0 TO NULL]", multiFacetResults[1].Results["Megapixels_Range"].Values[3].Range);
                Assert.Equal(0, multiFacetResults[1].Results["Megapixels_Range"].Values[3].Hits);
            }
        }

        [Fact]
        public void CanGetSuggestions()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                store.DatabaseCommands.Put("users/1", null, RavenJObject.FromObject(new User { Name = "John Smith" }), new RavenJObject { { "Raven-Entity-Name", "Users" } });
                store.DatabaseCommands.Put("users/2", null, RavenJObject.FromObject(new User { Name = "Jack Johnson" }), new RavenJObject { { "Raven-Entity-Name", "Users" } });
                store.DatabaseCommands.Put("users/3", null, RavenJObject.FromObject(new User { Name = "Robery Jones" }), new RavenJObject { { "Raven-Entity-Name", "Users" } });
                store.DatabaseCommands.Put("users/4", null, RavenJObject.FromObject(new User { Name = "David Jones" }), new RavenJObject { { "Raven-Entity-Name", "Users" } });
                WaitForIndexing(store);

                var suggestions = store.DatabaseCommands.Suggest(index.IndexName, new SuggestionQuery()
                {
                    Field = "Name",
                    Term = "<<johne davi>>",
                    Accuracy = 0.4f,
                    MaxSuggestions = 5,
                    Distance = StringDistanceTypes.JaroWinkler,
                    Popularity = true,
                });

                Assert.Equal("john", suggestions.Suggestions[0]);
                Assert.Equal("jones", suggestions.Suggestions[1]);
                Assert.Equal("johnson", suggestions.Suggestions[2]);
                Assert.Equal("david", suggestions.Suggestions[3]);
                Assert.Equal("jack", suggestions.Suggestions[4]);
            }
        }
    }
}
