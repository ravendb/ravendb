using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

using FastTests;

using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Data.Queries;
using Raven.Client.Indexing;
using Raven.Json.Linq;

using SlowTests.Core.Utils.Indexes;

using Xunit;

using Camera = SlowTests.Core.Utils.Entities.Camera;
using Company = SlowTests.Core.Utils.Entities.Company;
using Contact = SlowTests.Core.Utils.Entities.Contact;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Commands
{
    public class Querying : RavenTestBase
    {
        [Fact]
        public async Task CanDoSimpleQueryOnDatabase()
        {
            const string indexName = "CompaniesByName";
            using (var store = await GetDocumentStore())
            {
                var contact1 = new Contact { FirstName = "Expression Name" };
                var contact2 = new Contact { FirstName = "Expression First Name" };
                var contact3 = new Contact { FirstName = "First Name" };

                store.DatabaseCommands.Put("contacts/1", null, RavenJObject.FromObject(contact1), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });
                store.DatabaseCommands.Put("contacts/2", null, RavenJObject.FromObject(contact2), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });
                store.DatabaseCommands.Put("contacts/3", null, RavenJObject.FromObject(contact3), new RavenJObject { { "Raven-Entity-Name", "Contacts" } });

                store.DatabaseCommands.PutIndex(indexName, new IndexDefinition()
                {
                    Maps = { "from contact in docs.Contacts select new { contact.FirstName }" }
                }, false);
                WaitForIndexing(store);

                var companies = store.DatabaseCommands.Query(indexName, new IndexQuery { Query = "" });
                Assert.Equal(3, companies.TotalResults);
                Assert.Equal("Expression Name", companies.Results[0].Value<string>("FirstName"));
                Assert.Equal("Expression First Name", companies.Results[1].Value<string>("FirstName"));
                Assert.Equal("First Name", companies.Results[2].Value<string>("FirstName"));
            }
        }

        [Fact]
        public async Task CanProcessLongQueryString()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var entity1 = new Company { Name = "Async Company #1", Id = "companies/1" };
                    session.Store(entity1);
                    var entity2 = new Company { Name = "Async Company #2", Id = "companies/2" };
                    session.Store(entity2);

                    session.SaveChanges();

                    session.Advanced.DocumentStore.DatabaseCommands.PutIndex("Test", new IndexDefinition
                    {
                        Maps = { "from doc in docs.Companies select new { doc.Name }" }
                    }, true);

                    QueryResult query;
                    while (true)
                    {
                        query = session.Advanced.DocumentStore.DatabaseCommands.Query("Test", new IndexQuery());

                        if (query.IsStale == false)
                            break;

                        Thread.Sleep(100);
                    }
                }

                var stringBuilder = new StringBuilder();
                var maxLengthOfQueryUsingGetUrl = store.Conventions.MaxLengthOfQueryUsingGetUrl;
                while (stringBuilder.Length < maxLengthOfQueryUsingGetUrl)
                {
                    stringBuilder.Append(@"(Name: ""Async Company #1"") OR");
                }
                stringBuilder.Append(@"(Name: ""Async Company #2"")");
                //const string queryString = @"(((TagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""d278241e\-d6b2\-4d53\-bf42\-ae2786bb8307"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""53cd8b83\-8793\-4328\-a6f3\-d45755275766"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""7227bfa3\-1da2\-48d5\-aefb\-5716fe538173"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""66010435\-60bd\-4cb2\-bbba\-ef262c6f3b40"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""95cda7f6\-181c\-49f7\-809b\-9436011c7f29"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""6ce4f353\-93fa\-452c\-be97\-5c91c5a2a6bb"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""d278241e\-d6b2\-4d53\-bf42\-ae2786bb8307"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""fb638f78\-686d\-4d81\-b9f6\-332a1c936a36"") AND (AssociatedTagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9"")) OR ((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9""))))))))))))))))))";

                Assert.NotInRange(stringBuilder.Length, 0, store.Conventions.MaxLengthOfQueryUsingGetUrl);
                using (var session = store.OpenSession())
                {
                    var indexQuery = new IndexQuery { Start = 0, PageSize = 50, Query = stringBuilder.ToString() };
                    var queryResult = session.Advanced.DocumentStore.DatabaseCommands.Query("Test", indexQuery);
                    Assert.Equal(2, queryResult.TotalResults);
                }
            }
        }

        [Fact(Skip = "Missing feature: Boosting, Streaming")]
        public async Task CanStreamQueryResult()
        {
            using (var store = await GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                for (int i = 0; i < 30; i++)
                {
                    store.DatabaseCommands.Put("users/" + i, null, RavenJObject.FromObject(new User { Name = "Name" + i }), new RavenJObject { { "Raven-Entity-Name", "Users" } });
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

        [Fact(Skip = "Missing feature: Facets")]
        public async Task CanGetFacets()
        {
            using (var store = await GetDocumentStore())
            {
                var index = new CameraCost();
                index.Execute(store);

                for (int i = 0; i < 10; i++)
                {
                    store.DatabaseCommands.Put(
                        "cameras/" + i,
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

                var facetResults = store.DatabaseCommands.GetFacets(index.IndexName, new IndexQuery { Query = "" }, "facets/CameraFacets");

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

        [Fact(Skip = "Missing feature: Suggestions")]
        public async Task CanGetSuggestions()
        {
            using (var store = await GetDocumentStore())
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
