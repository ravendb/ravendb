using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10607 : RavenTestBase
    {
        public RavenDB_10607(ITestOutputHelper output) : base(output)
        {
        }

        private class Location
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public List<string> LocationParents { get; set; }
        }

        private class Result
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int? Depth { get; set; }
            public int? Depth2 { get; set; }
        }

        [Fact]
        public void CanUseCount1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Location
                    {
                        LocationParents = new List<string>
                        {
                            "locations/10", "locations/20"
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from location in session.Query<Location>()
                                select new Result
                                {
                                    Id = location.Id,
                                    Depth = location.LocationParents.Count, // Is a list
                                    Name = location.Name
                                };

                    Assert.Equal("from 'Locations' select id() as Id, LocationParents.Count as Depth, Name", query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].Depth);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseCount2(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Location
                    {
                        LocationParents = new List<string>
                        {
                            "locations/10", "locations/20"
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from location in session.Query<Location>()
                        select new Result
                        {
                            Id = location.Id,
                            Depth = location.LocationParents.Count(), // Is a list
                            Name = location.Name
                        };

                    Assert.Equal("from 'Locations' as location select " +
                                 "{ Id : id(location), Depth : (location?.LocationParents?.length??0), Name : location?.Name }"
                                 , query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].Depth);
                }
            }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanUseCount3(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Location
                    {
                        LocationParents = new List<string>
                        {
                            "locations/10", "locations/20"
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = from location in session.Query<Location>()
                        select new Result
                        {
                            Id = location.Id,
                            Depth = location.LocationParents.Count,
                            Depth2 = location.LocationParents.Count(),
                            Name = location.Name
                        };

                    Assert.Equal("from 'Locations' as location select { Id : id(location), " +
                                 "Depth : (location?.LocationParents?.length??0), " +
                                 "Depth2 : (location?.LocationParents?.length??0), " +
                                 "Name : location?.Name }"
                        , query.ToString());

                    var result = query.ToList();

                    Assert.Equal(1, result.Count);
                    Assert.Equal(2, result[0].Depth);
                    Assert.Equal(2, result[0].Depth2);
                }
            }
        }

    }
}
