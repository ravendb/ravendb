// -----------------------------------------------------------------------
//  <copyright file="TwoLocations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Indexes.Spatial;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Spatial
{
    public class TwoLocations : RavenTestBase
    {
        public TwoLocations(ITestOutputHelper output) : base(output)
        {
        }

        private class Event
        {
            public string Name;
            public Location[] Locations;

            public class Location
            {
                public double Lng, Lat;
            }
        }

        private static void Setup(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Event
                {
                    Name = "Trial",
                    Locations = new[]
                        {
                            new Event.Location
                            {
                                Lat =32.1067536,
                                Lng = 34.8357353
                            },
                            new Event.Location
                            {
                                Lat = 32.0624912,
                                Lng = 34.7700725
                            },
                        }
                });
                session.SaveChanges();
            }
        }

        [Fact]
        public void CanQueryByMultipleLocations()
        {
            using (var store = GetDocumentStore())
            {
                new MultiLocations().Execute(store);
                Setup(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocations>()
                        .Spatial("Location", factory => factory.WithinRadius(1, 32.0590291, 34.7707401))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.NotEmpty(list);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocations>()
                        .Spatial("Location", factory => factory.WithinRadius(1, 32.1104641, 34.8417456))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void CanQueryByMultipleLocations2()
        {
            using (var store = GetDocumentStore())
            {
                new MultiLocationsCustomFieldName().Execute(store);
                Setup(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.WithinRadius(1, 32.0590291, 34.7707401))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.NotEmpty(list);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.WithinRadius(1, 32.1104641, 34.8417456))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void CanQueryByMultipleLocationsOverHttp()
        {
            using (var store = GetDocumentStore())
            {
                new MultiLocations().Execute(store);
                Setup(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocations>()
                        .Spatial("Location", factory => factory.WithinRadius(1, 32.0590291, 34.7707401))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(list);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocations>()
                        .Spatial("Location", factory => factory.WithinRadius(1, 32.1104641, 34.8417456))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void CanQueryByMultipleLocationsHttp2()
        {
            using (var store = GetDocumentStore())
            {
                new MultiLocationsCustomFieldName().Execute(store);
                Setup(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.WithinRadius(1, 32.0590291, 34.7707401))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(list);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.WithinRadius(1, 32.1104641, 34.8417456))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void CanQueryByMultipleLocationsRaw()
        {
            using (var store = GetDocumentStore())
            {
                new MultiLocationsCustomFieldName().Execute(store);
                Setup(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.RelatesToShape("Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.NotEmpty(list);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.RelatesToShape("Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    RavenTestHelper.AssertNoIndexErrors(store);

                    Assert.NotEmpty(list);
                }
            }
        }

        [Fact]
        public void CanQueryByMultipleLocationsRawOverHttp()
        {
            using (var store = GetDocumentStore())
            {
                new MultiLocationsCustomFieldName().Execute(store);
                Setup(store);

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.RelatesToShape("Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(list);
                }

                using (var session = store.OpenSession())
                {
                    var list = session.Query<Event, MultiLocationsCustomFieldName>()
                        .Spatial("someField", factory => factory.RelatesToShape("Circle(34.770740 32.059029 d=1.000000)", SpatialRelation.Within))
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToList();

                    Assert.NotEmpty(list);
                }
            }
        }

        private class MultiLocations : AbstractIndexCreationTask<Event>
        {
            public MultiLocations()
            {
                Map = events =>
                      from e in events
                      select new
                      {
                          e.Name,
                          Location = e.Locations.Select(x => CreateSpatialField(x.Lat, x.Lng))
                      };
            }
        }

        private class MultiLocationsCustomFieldName : AbstractIndexCreationTask<Event>
        {
            public MultiLocationsCustomFieldName()
            {
                Map = events =>
                      from e in events
                      select new
                      {
                          e.Name,
                          someField = e.Locations.Select(x => CreateSpatialField(x.Lat, x.Lng))
                      };
            }
        }
    }
}
