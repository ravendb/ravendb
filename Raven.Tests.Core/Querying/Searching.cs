// -----------------------------------------------------------------------
//  <copyright file="Searching.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Tests.Core.Utils.Entities;
using Raven.Tests.Core.Utils.Indexes;
using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Core.Querying
{
    public class Searching : RavenCoreTestBase
    {
#if DNXCORE50
        public Searching(TestServerFixture fixture)
            : base(fixture)
        {

        }
#endif

        [Fact]
        public void CanSearchByMultipleTerms()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Posts/ByTitle", new IndexDefinition
                {
                    Map = "from post in docs.Posts select new { post.Title }",
                    Indexes = { { "Title", FieldIndexing.Analyzed } }
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Title = "Querying document database"
                    });

                    session.Store(new Post
                    {
                        Title = "Introduction to RavenDB"
                    });

                    session.Store(new Post
                    {
                        Title = "NOSQL databases"
                    });

                    session.Store(new Post
                    {
                        Title = "MSSQL 2012"
                    });

                    session.SaveChanges();

                    WaitForIndexing(store);

                    var aboutRavenDBDatabase =
                        session.Query<Post>("Posts/ByTitle")
                            .Search(x => x.Title, "database databases RavenDB")
                            .ToList();

                    Assert.Equal(3, aboutRavenDBDatabase.Count);

                    var exceptRavenDB =
                        session.Query<Post>("Posts/ByTitle")
                            .Search(x => x.Title, "RavenDB", options: SearchOptions.Not)
                            .ToList();

                    Assert.Equal(3, exceptRavenDB.Count);
                }
            }
        }

        [Fact]
        public void CanSearchByMultipleFields()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("Posts/ByTitleAndDescription", new IndexDefinition
                {
                    Map = "from post in docs.Posts select new { post.Title, post.Desc }",
                    Indexes = { { "Title", FieldIndexing.Analyzed }, { "Desc", FieldIndexing.Analyzed } }
                });

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Title = "RavenDB in action",
                        Desc = "Querying document database"
                    });

                    session.Store(new Post
                    {
                        Title = "Introduction to NOSQL",
                        Desc = "Modeling in document DB"
                    });

                    session.Store(new Post
                    {
                        Title = "MSSQL 2012"
                    });

                    session.SaveChanges();

                    WaitForIndexing(store);

                    var nosqlOrQuerying =
                        session.Query<Post>("Posts/ByTitleAndDescription")
                            .Search(x => x.Title, "nosql")
                            .Search(x => x.Desc, "querying")
                            .ToList();

                    Assert.Equal(2, nosqlOrQuerying.Count);
                    Assert.NotNull(nosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/1"));
                    Assert.NotNull(nosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/2"));

                    var notNosqlOrQuerying =
                        session.Query<Post>("Posts/ByTitleAndDescription")
                            .Search(x => x.Title, "nosql", options: SearchOptions.Not)
                            .Search(x => x.Desc, "querying")
                            .ToList();

                    Assert.Equal(2, notNosqlOrQuerying.Count);
                    Assert.NotNull(notNosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/1"));
                    Assert.NotNull(notNosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/3"));

                    var nosqlAndModeling =
                        session.Query<Post>("Posts/ByTitleAndDescription")
                            .Search(x => x.Title, "nosql")
                            .Search(x => x.Desc, "modeling", options: SearchOptions.And)
                            .ToList();

                    Assert.Equal(1, nosqlAndModeling.Count);
                    Assert.NotNull(nosqlAndModeling.FirstOrDefault(x => x.Id == "posts/2"));
                }
            }
        }

        [Fact]
        public void CanDoSpatialSearch()
        {
            using (var store = GetDocumentStore())
            {
                var eventsSpatialIndex = new Events_SpatialIndex();
                eventsSpatialIndex.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Event
                    {
                        Name = "Event1",
                        Latitude = 10.1234,
                        Longitude = 10.1234
                    });
                    session.Store(new Event
                    {
                        Name = "Event2",
                        Latitude = 0.3,
                        Longitude = 10.1234
                    });
                    session.Store(new Event
                    {
                        Name = "Event3",
                        Latitude = 19.1234,
                        Longitude = 10.789
                    });
                    session.Store(new Event
                    {
                        Name = "Event4",
                        Latitude = 10.1234,
                        Longitude = -0.2
                    });
                    session.Store(new Event
                    {
                        Name = "Event5",
                        Latitude = 10.1234,
                        Longitude = 19.789
                    });
                    session.Store(new Event
                    {
                        Name = "Event6",
                        Latitude = 60.1234,
                        Longitude = 19.789
                    });
                    session.Store(new Event
                    {
                        Name = "Event7",
                        Latitude = -60.1234,
                        Longitude = 19.789
                    });
                    session.Store(new Event
                    {
                        Name = "Event8",
                        Latitude = 10.1234,
                        Longitude = -19.789
                    });
                    session.Store(new Event
                    {
                        Name = "Event9",
                        Latitude = 10.1234,
                        Longitude = 79.789
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);


                    var events = session.Query<Event, Events_SpatialIndex>()
                        .Customize(x => x.WithinRadiusOf(
                            fieldName: "Coordinates",
                            radius: 1243.0, //km
                            latitude: 10.1230,
                            longitude: 10.1230))
                        .OrderBy(x => x.Name)
                        .ToArray();

                    Assert.Equal(5, events.Length);
                    Assert.Equal("Event1", events[0].Name);
                    Assert.Equal("Event2", events[1].Name);
                    Assert.Equal("Event3", events[2].Name);
                    Assert.Equal("Event4", events[3].Name);
                    Assert.Equal("Event5", events[4].Name);
                }
            }
        }

        [Fact]
        public void CanDoSearchBoosting()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "Bob",
                        LastName = "LastName"
                    });
                    session.Store(new User
                    {
                        Name = "Name",
                        LastName = "LastName"
                    });
                    session.Store(new User
                    {
                        Name = "Name",
                        LastName = "Bob"
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var users = session.Query<User, Users_ByName>()
                        .Where(x => x.Name == "Bob" || x.LastName == "Bob")
                        .ToArray();
                    WaitForUserToContinueTheTest(store);
                    Assert.Equal(2, users.Length);
                    Assert.Equal("Name", users[0].Name);
                    Assert.Equal("Bob", users[1].Name);
                }
            }
        }

        [Fact]
        public void CanProvideSuggestionsAndLazySuggestions()
        {
            using (var store = GetDocumentStore())
            {
                new Users_ByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "John Smith"
                    });
                    session.Store(new User
                    {
                        Name = "Jack Johnson"
                    });
                    session.Store(new User
                    {
                        Name = "Robery Jones"
                    });
                    session.Store(new User
                    {
                        Name = "David Jones"
                    });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var users = session.Query<User, Users_ByName>()
                        .Where(x => x.Name == "johne");

                    SuggestionQueryResult suggestionResult = users.Suggest();
                    Assert.Equal(3, suggestionResult.Suggestions.Length);
                    Assert.Equal("john", suggestionResult.Suggestions[0]);
                    Assert.Equal("jones", suggestionResult.Suggestions[1]);
                    Assert.Equal("johnson", suggestionResult.Suggestions[2]);


                    Lazy<SuggestionQueryResult> lazySuggestionResult = users.SuggestLazy();

                    Assert.False(lazySuggestionResult.IsValueCreated);

                    suggestionResult = lazySuggestionResult.Value;

                    Assert.Equal(3, suggestionResult.Suggestions.Length);
                    Assert.Equal("john", suggestionResult.Suggestions[0]);
                    Assert.Equal("jones", suggestionResult.Suggestions[1]);
                    Assert.Equal("johnson", suggestionResult.Suggestions[2]);
                }
            }
        }

        [Fact]
        public void CanPerformFacetedSearchAndLazyFacatedSearch()
        {
            using (var store = GetDocumentStore())
            {
                new CameraCost().Execute(store);

                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Camera
                        {
                            Id = "cameras/" + i,
                            Manufacturer = i % 2 == 0 ? "Manufacturer1" : "Manufacturer2",
                            Cost = i * 100D,
                            Megapixels = i * 1D
                        });
                    }

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
                    session.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets });
                    session.SaveChanges();
                    WaitForIndexing(store);

                    var facetResults = session.Query<Camera, CameraCost>()
                        .ToFacets("facets/CameraFacets");

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

                    var lazyFacetResults = session.Query<Camera, CameraCost>()
                        .ToFacetsLazy("facets/CameraFacets");

                    Assert.False(lazyFacetResults.IsValueCreated);

                    facetResults = lazyFacetResults.Value;

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
                }
            }
        }

        [Fact]
        public void CanSearchOnFullyAnalyzedTerm()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Object_ByName());
                using (var session = store.OpenSession())
                {
                        session.Query<NamedObject, Object_ByName>()
                            .Search(x => x.Name, "AND")
                            .ToList();
                }
                //This test pass if it does not throw
            }
        }
        public class Object_ByName : AbstractIndexCreationTask<NamedObject>
        {
            public Object_ByName()
            {
                Map = objects => objects.Select(x => new {x.Name});

                Indexes.Add(x => x.Name, FieldIndexing.Analyzed);
                Analyzers.Add(x => x.Name, "Lucene.Net.Analysis.Standard.StandardAnalyzer");
            }
        }

        public class NamedObject
        {
            public string Name { get; set; }
        }
    }
}
