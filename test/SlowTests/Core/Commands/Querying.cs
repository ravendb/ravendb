using System.Collections.Generic;
using System.Text;

using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Suggestion;
using SlowTests.Core.Utils.Indexes;
using Sparrow.Json;
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
        public void CanDoSimpleQueryOnDatabase()
        {
            const string indexName = "CompaniesByName";
            using (var store = GetDocumentStore())
            {
                var contact1 = new Contact { FirstName = "Expression Name" };
                var contact2 = new Contact { FirstName = "Expression First Name" };
                var contact3 = new Contact { FirstName = "First Name" };

                using (var commands = store.Commands())
                {
                    commands.Put("contacts/1", null, contact1, new Dictionary<string, object> { { "@collection", "Contacts" } });
                    commands.Put("contacts/2", null, contact2, new Dictionary<string, object> { { "@collection", "Contacts" } });
                    commands.Put("contacts/3", null, contact3, new Dictionary<string, object> { { "@collection", "Contacts" } });

                    store.Admin.Send(new PutIndexesOperation(new IndexDefinition
                    {
                        Maps = { "from contact in docs.Contacts select new { contact.FirstName }" },
                        Name = indexName
                    }));

                    WaitForIndexing(store);

                    var companies = commands.Query(new IndexQuery { Query = $"FROM INDEX '{indexName}'" });
                    Assert.Equal(3, companies.TotalResults);

                    var company = (BlittableJsonReaderObject)companies.Results[0];
                    string firstName;
                    Assert.True(company.TryGet("FirstName", out firstName));
                    Assert.Equal("Expression Name", firstName);

                    company = (BlittableJsonReaderObject)companies.Results[1];
                    Assert.True(company.TryGet("FirstName", out firstName));
                    Assert.Equal("Expression First Name", firstName);

                    company = (BlittableJsonReaderObject)companies.Results[2];
                    Assert.True(company.TryGet("FirstName", out firstName));
                    Assert.Equal("First Name", firstName);
                }
            }
        }

        [Fact]
        public void CanProcessLongQueryString()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var entity1 = new Company { Name = "Async Company #1", Id = "companies/1" };
                    session.Store(entity1);
                    var entity2 = new Company { Name = "Async Company #2", Id = "companies/2" };
                    session.Store(entity2);

                    session.SaveChanges();

                    store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                    {
                        Maps = { "from doc in docs.Companies select new { doc.Name }" },
                        Name = "Test"
                    }}));

                    WaitForIndexing(store);
                }

                var stringBuilder = new StringBuilder("FROM INDEX 'Test' WHERE");
                var maxLengthOfQueryUsingGetUrl = store.Conventions.MaxLengthOfQueryUsingGetUrl;
                while (stringBuilder.Length < maxLengthOfQueryUsingGetUrl)
                {
                    stringBuilder.Append(@" (Name = 'Async Company #1') OR");
                }
                stringBuilder.Append(@" (Name = 'Async Company #2')");
                //const string queryString = @"(((TagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""d278241e\-d6b2\-4d53\-bf42\-ae2786bb8307"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9"") AND (AssociatedTagID: ""0f7e407f\-46c8\-4dcc\-bcae\-512934732af5"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""53cd8b83\-8793\-4328\-a6f3\-d45755275766"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""7227bfa3\-1da2\-48d5\-aefb\-5716fe538173"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""66010435\-60bd\-4cb2\-bbba\-ef262c6f3b40"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""95cda7f6\-181c\-49f7\-809b\-9436011c7f29"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""6ce4f353\-93fa\-452c\-be97\-5c91c5a2a6bb"") AND (AssociatedTagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"")) OR (((TagID: ""6ba9d9d1\-6b33\-40df\-b0fe\-5091790d9519"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""d278241e\-d6b2\-4d53\-bf42\-ae2786bb8307"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9"") AND (AssociatedTagID: ""4e470c6a\-b2cc\-47ba\-a8c6\-0e84cc3c2f98"")) OR (((TagID: ""fb638f78\-686d\-4d81\-b9f6\-332a1c936a36"") AND (AssociatedTagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9"")) OR ((TagID: ""bd7ad8b4\-f9df\-4aa0\-9a18\-9517bfd4bd83"") AND (AssociatedTagID: ""ba59490f\-7003\-463b\-bb3d\-7ffd3f016af9""))))))))))))))))))";

                Assert.NotInRange(stringBuilder.Length, 0, store.Conventions.MaxLengthOfQueryUsingGetUrl);
                var indexQuery = new IndexQuery { Start = 0, PageSize = 50, Query = stringBuilder.ToString() };

                using (var commands = store.Commands())
                {
                    var queryResult = commands.Query(indexQuery);
                    Assert.Equal(2, queryResult.TotalResults);
                }
            }
        }

        [Fact]
        public void CanStreamQueryResult()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                using (var commands = store.Commands())
                {
                    for (int i = 0; i < 30; i++)
                    {
                        commands.Put("users/" + i, null, new User { Name = "Name" + i }, new Dictionary<string, object> { { "@collection", "Users" } });
                    }
                }

                WaitForIndexing(store);

                var count = 0;
                using (var session = store.OpenSession())
                {
                    var enumerator = session.Advanced.Stream(session.Query<User, Users_ByName>());
                    while (enumerator.MoveNext())
                    {
                        count++;
                    }
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

                using (var commands = store.Commands())
                {
                    for (var i = 0; i < 10; i++)
                    {
                        commands.Put(
                            "cameras/" + i,
                            null,
                            new Camera
                            {
                                Id = "cameras/" + i,
                                Manufacturer = i % 2 == 0 ? "Manufacturer1" : "Manufacturer2",
                                Cost = i * 100D,
                                Megapixels = i * 1D
                            },
                            new Dictionary<string, object> { { "@collection", "Cameras" } });
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
                            Name = "Cost_D_Range",
                            Mode = FacetMode.Ranges,
                            Ranges =
                            {
                                "[NULL TO 200.0]",
                                "[300.0 TO 400.0]",
                                "[500.0 TO 600.0]",
                                "[700.0 TO 800.0]",
                                "[900.0 TO NULL]"
                            }
                        },
                        new Facet
                        {
                            Name = "Megapixels_D_Range",
                            Mode = FacetMode.Ranges,
                            Ranges =
                            {
                                "[NULL TO 3.0]",
                                "[4.0 TO 7.0]",
                                "[8.0 TO 10.0]",
                                "[11.0 TO NULL]"
                            }
                        }
                    };

                    commands.Put(
                        "facets/CameraFacets",
                        null,
                        new FacetSetup { Id = "facets/CameraFacets", Facets = facets },
                        null);

                    WaitForIndexing(store);

                    using (var session = store.OpenSession())
                    {
                        var facetResults = session
                            .Query<Camera, CameraCost>()
                            .ToFacets("facets/CameraFacets");

                        Assert.Equal(3, facetResults.Results.Count);

                        Assert.Equal(2, facetResults.Results["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", facetResults.Results["Manufacturer"].Values[0].Range);
                        Assert.Equal(5, facetResults.Results["Manufacturer"].Values[0].Hits);
                        Assert.Equal("manufacturer2", facetResults.Results["Manufacturer"].Values[1].Range);
                        Assert.Equal(5, facetResults.Results["Manufacturer"].Values[1].Hits);

                        Assert.Equal(5, facetResults.Results["Cost_D_Range"].Values.Count);
                        Assert.Equal("[NULL TO 200.0]", facetResults.Results["Cost_D_Range"].Values[0].Range);
                        Assert.Equal(3, facetResults.Results["Cost_D_Range"].Values[0].Hits);
                        Assert.Equal("[300.0 TO 400.0]", facetResults.Results["Cost_D_Range"].Values[1].Range);
                        Assert.Equal(2, facetResults.Results["Cost_D_Range"].Values[1].Hits);
                        Assert.Equal("[500.0 TO 600.0]", facetResults.Results["Cost_D_Range"].Values[2].Range);
                        Assert.Equal(2, facetResults.Results["Cost_D_Range"].Values[2].Hits);
                        Assert.Equal("[700.0 TO 800.0]", facetResults.Results["Cost_D_Range"].Values[3].Range);
                        Assert.Equal(2, facetResults.Results["Cost_D_Range"].Values[3].Hits);
                        Assert.Equal("[900.0 TO NULL]", facetResults.Results["Cost_D_Range"].Values[4].Range);
                        Assert.Equal(1, facetResults.Results["Cost_D_Range"].Values[4].Hits);

                        Assert.Equal(4, facetResults.Results["Megapixels_D_Range"].Values.Count);
                        Assert.Equal("[NULL TO 3.0]", facetResults.Results["Megapixels_D_Range"].Values[0].Range);
                        Assert.Equal(4, facetResults.Results["Megapixels_D_Range"].Values[0].Hits);
                        Assert.Equal("[4.0 TO 7.0]", facetResults.Results["Megapixels_D_Range"].Values[1].Range);
                        Assert.Equal(4, facetResults.Results["Megapixels_D_Range"].Values[1].Hits);
                        Assert.Equal("[8.0 TO 10.0]", facetResults.Results["Megapixels_D_Range"].Values[2].Range);
                        Assert.Equal(2, facetResults.Results["Megapixels_D_Range"].Values[2].Hits);
                        Assert.Equal("[11.0 TO NULL]", facetResults.Results["Megapixels_D_Range"].Values[3].Range);
                        Assert.Equal(0, facetResults.Results["Megapixels_D_Range"].Values[3].Hits);
                    }

                    using (var session = store.OpenSession())
                    {
                        var multiFacetResults = session.Advanced.DocumentStore.Operations.Send(new GetMultiFacetsOperation(new FacetQuery()
                        {
                            IndexName = index.IndexName,
                            Query = "Cost:{NULL TO 200}",
                            FacetSetupDoc = "facets/CameraFacets"
                        }, new FacetQuery()
                        {
                            IndexName = index.IndexName,
                            Query = "Megapixels:{NULL TO 3}",
                            FacetSetupDoc = "facets/CameraFacets"
                        }));

                        Assert.Equal(3, multiFacetResults[0].Results.Count);

                        Assert.Equal(2, multiFacetResults[0].Results["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[0].Results["Manufacturer"].Values[0].Range);
                        Assert.Equal(1, multiFacetResults[0].Results["Manufacturer"].Values[0].Hits);
                        Assert.Equal("manufacturer2", multiFacetResults[0].Results["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[0].Results["Manufacturer"].Values[1].Hits);

                        Assert.Equal(5, multiFacetResults[0].Results["Cost_D_Range"].Values.Count);
                        Assert.Equal("[NULL TO 200.0]", multiFacetResults[0].Results["Cost_D_Range"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0].Results["Cost_D_Range"].Values[0].Hits);
                        Assert.Equal("[300.0 TO 400.0]", multiFacetResults[0].Results["Cost_D_Range"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0].Results["Cost_D_Range"].Values[1].Hits);
                        Assert.Equal("[500.0 TO 600.0]", multiFacetResults[0].Results["Cost_D_Range"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0].Results["Cost_D_Range"].Values[2].Hits);
                        Assert.Equal("[700.0 TO 800.0]", multiFacetResults[0].Results["Cost_D_Range"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0].Results["Cost_D_Range"].Values[3].Hits);
                        Assert.Equal("[900.0 TO NULL]", multiFacetResults[0].Results["Cost_D_Range"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[0].Results["Cost_D_Range"].Values[4].Hits);

                        Assert.Equal(4, multiFacetResults[0].Results["Megapixels_D_Range"].Values.Count);
                        Assert.Equal("[NULL TO 3.0]", multiFacetResults[0].Results["Megapixels_D_Range"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0].Results["Megapixels_D_Range"].Values[0].Hits);
                        Assert.Equal("[4.0 TO 7.0]", multiFacetResults[0].Results["Megapixels_D_Range"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0].Results["Megapixels_D_Range"].Values[1].Hits);
                        Assert.Equal("[8.0 TO 10.0]", multiFacetResults[0].Results["Megapixels_D_Range"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0].Results["Megapixels_D_Range"].Values[2].Hits);
                        Assert.Equal("[11.0 TO NULL]", multiFacetResults[0].Results["Megapixels_D_Range"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0].Results["Megapixels_D_Range"].Values[3].Hits);


                        Assert.Equal(3, multiFacetResults[1].Results.Count);

                        Assert.Equal(2, multiFacetResults[1].Results["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[1].Results["Manufacturer"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[1].Results["Manufacturer"].Values[0].Hits);
                        Assert.Equal("manufacturer2", multiFacetResults[1].Results["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[1].Results["Manufacturer"].Values[1].Hits);

                        Assert.Equal(5, multiFacetResults[1].Results["Cost_D_Range"].Values.Count);
                        Assert.Equal("[NULL TO 200.0]", multiFacetResults[1].Results["Cost_D_Range"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1].Results["Cost_D_Range"].Values[0].Hits);
                        Assert.Equal("[300.0 TO 400.0]", multiFacetResults[1].Results["Cost_D_Range"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1].Results["Cost_D_Range"].Values[1].Hits);
                        Assert.Equal("[500.0 TO 600.0]", multiFacetResults[1].Results["Cost_D_Range"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1].Results["Cost_D_Range"].Values[2].Hits);
                        Assert.Equal("[700.0 TO 800.0]", multiFacetResults[1].Results["Cost_D_Range"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1].Results["Cost_D_Range"].Values[3].Hits);
                        Assert.Equal("[900.0 TO NULL]", multiFacetResults[1].Results["Cost_D_Range"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[1].Results["Cost_D_Range"].Values[4].Hits);

                        Assert.Equal(4, multiFacetResults[1].Results["Megapixels_D_Range"].Values.Count);
                        Assert.Equal("[NULL TO 3.0]", multiFacetResults[1].Results["Megapixels_D_Range"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1].Results["Megapixels_D_Range"].Values[0].Hits);
                        Assert.Equal("[4.0 TO 7.0]", multiFacetResults[1].Results["Megapixels_D_Range"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1].Results["Megapixels_D_Range"].Values[1].Hits);
                        Assert.Equal("[8.0 TO 10.0]", multiFacetResults[1].Results["Megapixels_D_Range"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1].Results["Megapixels_D_Range"].Values[2].Hits);
                        Assert.Equal("[11.0 TO NULL]", multiFacetResults[1].Results["Megapixels_D_Range"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1].Results["Megapixels_D_Range"].Values[3].Hits);
                    }
                }
            }
        }

        [Fact]
        public void CanGetSuggestions()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Users_ByName();
                index.Execute(store);

                using (var commands = store.Commands())
                {
                    commands.Put("users/1", null, new User { Name = "John Smith" }, new Dictionary<string, object> { { "@collection", "Users" } });
                    commands.Put("users/2", null, new User { Name = "Jack Johnson" }, new Dictionary<string, object> { { "@collection", "Users" } });
                    commands.Put("users/3", null, new User { Name = "Robery Jones" }, new Dictionary<string, object> { { "@collection", "Users" } });
                    commands.Put("users/4", null, new User { Name = "David Jones" }, new Dictionary<string, object> { { "@collection", "Users" } });
                }

                WaitForIndexing(store);


                using (var session = store.OpenSession())
                {
                    var suggestions = session.Query<User, Users_ByName>().Suggest(new SuggestionQuery()
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
}
