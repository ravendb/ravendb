// -----------------------------------------------------------------------
//  <copyright file="Searching.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Collections.Generic;
using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;
using SlowTests.Core.Utils.Indexes;

using Xunit;

using Camera = SlowTests.Core.Utils.Entities.Camera;
using Event = SlowTests.Core.Utils.Entities.Event;
using Post = SlowTests.Core.Utils.Entities.Post;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Core.Querying
{
    public class Searching : RavenTestBase
    {
        public Searching(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSearchByMultipleTerms()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Maps = { "from post in docs.Posts select new { post.Title }" },
                    Fields = { { "Title", new IndexFieldOptions { Indexing = FieldIndexing.Search } } },
                    Name = "Posts/ByTitle"
                }}));

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

                    Indexes.WaitForIndexing(store);

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
                store.Maintenance.Send(new PutIndexesOperation(new[] { new IndexDefinition
                {
                    Maps = { "from post in docs.Posts select new { post.Title, post.Desc }" },
                    Fields =
                    {
                        { "Title", new IndexFieldOptions { Indexing = FieldIndexing.Search} },
                        { "Desc", new IndexFieldOptions { Indexing = FieldIndexing.Search} }
                    },
                    Name = "Posts/ByTitleAndDescription"
                }}));

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

                    Indexes.WaitForIndexing(store);

                    var nosqlOrQuerying =
                        session.Query<Post>("Posts/ByTitleAndDescription")
                            .Search(x => x.Title, "nosql")
                            .Search(x => x.Desc, "querying")
                            .ToList();

                    Assert.Equal(2, nosqlOrQuerying.Count);
                    Assert.NotNull(nosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/1-A"));
                    Assert.NotNull(nosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/2-A"));

                    var notNosqlOrQuerying =
                        session.Query<Post>("Posts/ByTitleAndDescription")
                            .Search(x => x.Title, "nosql", options: SearchOptions.Not)
                            .Search(x => x.Desc, "querying")
                            .ToList();

                    Assert.Equal(2, notNosqlOrQuerying.Count);
                    Assert.NotNull(notNosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/1-A"));
                    Assert.NotNull(notNosqlOrQuerying.FirstOrDefault(x => x.Id == "posts/3-A"));

                    var nosqlAndModeling =
                        session.Query<Post>("Posts/ByTitleAndDescription")
                            .Search(x => x.Title, "nosql")
                            .Search(x => x.Desc, "modeling", options: SearchOptions.And)
                            .ToList();

                    Assert.Equal(1, nosqlAndModeling.Count);
                    Assert.NotNull(nosqlAndModeling.FirstOrDefault(x => x.Id == "posts/2-A"));
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
                    Indexes.WaitForIndexing(store);


                    var events = session.Query<Events_SpatialIndex.Result, Events_SpatialIndex>()
                        .Spatial(x => x.Coordinates, x => x.WithinRadius(1243.0, 10.1230, 10.1230))
                        .OrderBy(x => x.Name)
                        .OfType<Event>()
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
                    Indexes.WaitForIndexing(store);

                    var users = session.Query<User, Users_ByName>()
                        .Where(x => x.Name == "Bob" || x.LastName == "Bob")
                        .ToArray();

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
                    Indexes.WaitForIndexing(store);

                    var suggestionResult = session.Query<User, Users_ByName>()
                        .SuggestUsing(f => f.ByField(x => x.Name, "johne"))
                        .Execute();

                    Assert.Equal(3, suggestionResult["Name"].Suggestions.Count);
                    Assert.Equal("john", suggestionResult["Name"].Suggestions[0]);
                    Assert.Equal("jones", suggestionResult["Name"].Suggestions[1]);
                    Assert.Equal("johnson", suggestionResult["Name"].Suggestions[2]);

                    var lazySuggestionResult = session.Query<User, Users_ByName>()
                        .SuggestUsing(f => f.ByField(x => x.Name, "johne"))
                        .ExecuteLazy();

                    Assert.False(lazySuggestionResult.IsValueCreated);

                    suggestionResult = lazySuggestionResult.Value;

                    Assert.Equal(3, suggestionResult["Name"].Suggestions.Count);
                    Assert.Equal("john", suggestionResult["Name"].Suggestions[0]);
                    Assert.Equal("jones", suggestionResult["Name"].Suggestions[1]);
                    Assert.Equal("johnson", suggestionResult["Name"].Suggestions[2]);
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
                                "Megapixels >= 11"
                            }
                        }
                    };
                    session.Store(new FacetSetup { Id = "facets/CameraFacets", Facets = facets, RangeFacets = rangeFacets });
                    session.SaveChanges();
                    Indexes.WaitForIndexing(store);

                    var facetResults = session.Query<Camera, CameraCost>()
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

                    var lazyFacetResults = session.Query<Camera, CameraCost>()
                        .AggregateUsing("facets/CameraFacets")
                        .ExecuteLazy();

                    Assert.False(lazyFacetResults.IsValueCreated);

                    facetResults = lazyFacetResults.Value;

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
            }
        }
    }
}
