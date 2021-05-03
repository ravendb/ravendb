using System.Collections.Generic;
using System.Linq;
using System.Text;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Queries.Facets;
using Raven.Client.Documents.Queries.Suggestions;
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
        public Querying(ITestOutputHelper output) : base(output)
        {
        }

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

                    store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
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

                    store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                    {
                        Maps = { "from doc in docs.Companies select new { doc.Name }" },
                        Name = "Test"
                    }}));

                    WaitForIndexing(store);
                }

                var stringBuilder = new StringBuilder("FROM INDEX 'Test' WHERE");
                while (stringBuilder.Length < 16 * 1024)
                {
                    stringBuilder.Append(@" (Name = 'Async Company #1') OR");
                }
                stringBuilder.Append(@" (Name = 'Async Company #2') LIMIT 50 OFFSET 0");

                var indexQuery = new IndexQuery { Query = stringBuilder.ToString() };

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
                            FieldName = "Manufacturer"
                        }
                    };

                    var rangeFacets = new List<RangeFacet>
                    {
                        new RangeFacet
                        {
                            Ranges =
                            {
                                "Cost <= 200",
                                "Cost >= 300 and Cost <= 400",
                                "Cost >= 500 and Cost <= 600",
                                "Cost >= 700 and Cost <= 800",
                                "Cost >= 900"
                            }
                        },
                        new RangeFacet
                        {
                            Ranges =
                            {
                                "Megapixels <= 3",
                                "Megapixels >= 4 and Megapixels <= 7",
                                "Megapixels >= 8 and Megapixels <= 10",
                                "Megapixels >= 11",
                            }
                        }
                    };


                    commands.Put(
                        "facets/CameraFacets",
                        null,
                        new FacetSetup { Id = "facets/CameraFacets", Facets = facets, RangeFacets = rangeFacets },
                        null);

                    WaitForIndexing(store);

                    using (var session = store.OpenSession())
                    {
                        var facetResults = session
                            .Query<Camera, CameraCost>()
                            .AggregateUsing("facets/CameraFacets")
                            .Execute();

                        Assert.Equal(3, facetResults.Count);

                        Assert.Equal(2, facetResults["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", facetResults["Manufacturer"].Values[0].Range);
                        Assert.Equal(5, facetResults["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", facetResults["Manufacturer"].Values[1].Range);
                        Assert.Equal(5, facetResults["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, facetResults["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", facetResults["Cost"].Values[0].Range);
                        Assert.Equal(3, facetResults["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", facetResults["Cost"].Values[1].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", facetResults["Cost"].Values[2].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", facetResults["Cost"].Values[3].Range);
                        Assert.Equal(2, facetResults["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", facetResults["Cost"].Values[4].Range);
                        Assert.Equal(1, facetResults["Cost"].Values[4].Count);

                        Assert.Equal(4, facetResults["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", facetResults["Megapixels"].Values[0].Range);
                        Assert.Equal(4, facetResults["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", facetResults["Megapixels"].Values[1].Range);
                        Assert.Equal(4, facetResults["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", facetResults["Megapixels"].Values[2].Range);
                        Assert.Equal(2, facetResults["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", facetResults["Megapixels"].Values[3].Range);
                        Assert.Equal(0, facetResults["Megapixels"].Values[3].Count);
                    }

                    using (var session = store.OpenSession())
                    {
                        var r1 = session.Query<Camera, CameraCost>()
                            .Where(x => x.Cost < 200)
                            .AggregateUsing("facets/CameraFacets")
                            .Execute();

                        var r2 = session.Query<Camera, CameraCost>()
                            .Where(x => x.Megapixels < 3)
                            .AggregateUsing("facets/CameraFacets")
                            .Execute();

                        var multiFacetResults = new[] { r1, r2 };

                        Assert.Equal(3, multiFacetResults[0].Count);

                        Assert.Equal(2, multiFacetResults[0]["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[0]["Manufacturer"].Values[0].Range);
                        Assert.Equal(1, multiFacetResults[0]["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", multiFacetResults[0]["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[0]["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, multiFacetResults[0]["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", multiFacetResults[0]["Cost"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0]["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", multiFacetResults[0]["Cost"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", multiFacetResults[0]["Cost"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", multiFacetResults[0]["Cost"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", multiFacetResults[0]["Cost"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[0]["Cost"].Values[4].Count);

                        Assert.Equal(4, multiFacetResults[0]["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", multiFacetResults[0]["Megapixels"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[0]["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", multiFacetResults[0]["Megapixels"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", multiFacetResults[0]["Megapixels"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", multiFacetResults[0]["Megapixels"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[0]["Megapixels"].Values[3].Count);


                        Assert.Equal(3, multiFacetResults[1].Count);

                        Assert.Equal(2, multiFacetResults[1]["Manufacturer"].Values.Count);
                        Assert.Equal("manufacturer1", multiFacetResults[1]["Manufacturer"].Values[0].Range);
                        Assert.Equal(2, multiFacetResults[1]["Manufacturer"].Values[0].Count);
                        Assert.Equal("manufacturer2", multiFacetResults[1]["Manufacturer"].Values[1].Range);
                        Assert.Equal(1, multiFacetResults[1]["Manufacturer"].Values[1].Count);

                        Assert.Equal(5, multiFacetResults[1]["Cost"].Values.Count);
                        Assert.Equal("Cost <= 200", multiFacetResults[1]["Cost"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1]["Cost"].Values[0].Count);
                        Assert.Equal("Cost >= 300 and Cost <= 400", multiFacetResults[1]["Cost"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[1].Count);
                        Assert.Equal("Cost >= 500 and Cost <= 600", multiFacetResults[1]["Cost"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[2].Count);
                        Assert.Equal("Cost >= 700 and Cost <= 800", multiFacetResults[1]["Cost"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[3].Count);
                        Assert.Equal("Cost >= 900", multiFacetResults[1]["Cost"].Values[4].Range);
                        Assert.Equal(0, multiFacetResults[1]["Cost"].Values[4].Count);

                        Assert.Equal(4, multiFacetResults[1]["Megapixels"].Values.Count);
                        Assert.Equal("Megapixels <= 3", multiFacetResults[1]["Megapixels"].Values[0].Range);
                        Assert.Equal(3, multiFacetResults[1]["Megapixels"].Values[0].Count);
                        Assert.Equal("Megapixels >= 4 and Megapixels <= 7", multiFacetResults[1]["Megapixels"].Values[1].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[1].Count);
                        Assert.Equal("Megapixels >= 8 and Megapixels <= 10", multiFacetResults[1]["Megapixels"].Values[2].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[2].Count);
                        Assert.Equal("Megapixels >= 11", multiFacetResults[1]["Megapixels"].Values[3].Range);
                        Assert.Equal(0, multiFacetResults[1]["Megapixels"].Values[3].Count);
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
                    var suggestions = session.Query<User, Users_ByName>()
                        .SuggestUsing(f => f.ByField("Name", new[] { "johne", "davi" }).WithOptions(new SuggestionOptions
                        {
                            PageSize = 5,
                            Distance = StringDistanceTypes.JaroWinkler,
                            SortMode = SuggestionSortMode.Popularity,
                            Accuracy = 0.4f
                        }))
                        .Execute();

                    Assert.Equal("john", suggestions["Name"].Suggestions[0]);
                    Assert.Equal("jones", suggestions["Name"].Suggestions[1]);
                    Assert.Equal("johnson", suggestions["Name"].Suggestions[2]);
                    Assert.Equal("david", suggestions["Name"].Suggestions[3]);
                    Assert.Equal("jack", suggestions["Name"].Suggestions[4]);
                }
            }
        }
    }
}
