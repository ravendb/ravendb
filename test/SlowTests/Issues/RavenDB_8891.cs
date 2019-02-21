using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_8891 : RavenTestBase
    {
        [Fact]
        public void Can_query_multidimentional_array()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Song
                    {
                        Tags = new List<List<string>>
                        {
                            new List<string>
                            {
                                "Elektro House",
                                "100"
                            }
                        }
                    });

                    session.SaveChanges();

                    var results = session.Advanced.RawQuery<Song>(@"from Songs where Tags[][] == ""Elektro House""").WaitForNonStaleResults().ToList();

                    Assert.Equal(1, results.Count);

                    var q = session.Query<Song>().Customize(x => x.WaitForNonStaleResults()).Where(x => x.Tags.Any(y => y.Any(z => z == "Elektro House")));

                    Assert.Equal(@"from Songs where Tags = $p0", q.ToString());

                    results = q.ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        [Fact]
        public void Can_query_3_dimensional_array_via_client()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Album
                    {
                        Tags3D = new List<List<List<string>>>
                        {
                            new List<List<string>>
                            {
                                new List<string>
                                {
                                    "Elektro House",
                                    "100"
                                }
                            }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Album>().Where(x => x.Tags3D.Any(t1 => t1.Any(t2 => t2.Any(t3 => t3 == "Elektro House"))));

                    Assert.Equal("from Albums where Tags3D = $p0", q.ToString());

                    var results = q.ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        [Fact]
        public void Can_query_4_dimensional_array_via_client()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Album
                    {
                        Tags4D = new List<List<List<List<string>>>>
                        {
                            new List<List<List<string>>>
                            {
                                new List<List<string>>
                                {
                                    new List<string>
                                    {
                                        "Elektro House",
                                        "100"
                                    }
                                }
                            }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Album>().Where(x => x.Tags4D.Any(t1 => t1.Any(t2 => t2.Any(t3 => t3.Any(t4 => t4 == "Elektro House")))));

                    Assert.Equal("from Albums where Tags4D = $p0", q.ToString());

                    var results = q.ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        [Fact]
        public void Can_query_4_dimensional_array_using_contains_via_client()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Album
                    {
                        Tags4D = new List<List<List<List<string>>>>
                        {
                            new List<List<List<string>>>
                            {
                                new List<List<string>>
                                {
                                    new List<string>
                                    {
                                        "Elektro House",
                                        "100"
                                    }
                                }
                            }
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var q = session.Query<Album>().Where(x => x.Tags4D.Any(t1 => t1.Any(t2 => t2.Any(t3 => t3.Contains("Elektro House")))));

                    Assert.Equal("from Albums where Tags4D = $p0", q.ToString());

                    var results = q.ToList();

                    Assert.Equal(1, results.Count);
                }
            }
        }

        private class Song
        {
            public List<List<string>> Tags { get; set; }
        }

        private class Album
        {
            public List<List<List<string>>> Tags3D { get; set; }
            public List<List<List<List<string>>>> Tags4D { get; set; }
        }
    }
}
