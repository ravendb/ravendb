using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Database.Extensions;
using Raven.Http;
using Xunit;
using Raven.Client.Document;
using Raven.Database.Server;
using System.Threading;
using System.IO;

namespace Raven.Client.Tests.Querying
{
    public class UsingDynamicQueryWithRemoteServer : RemoteClientTest, IDisposable
    {   
		private readonly string path;
        private readonly int port;

		public UsingDynamicQueryWithRemoteServer()
		{
            port = 8080;
            path = GetPath("TestDb");
			NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(8080);
		}

		#region IDisposable Members

		public void Dispose()
		{
            IOExtensions.DeleteDirectory(path);
		}

		#endregion

        [Fact]
        public void CanPerformDynamicQueryUsingClientLinqQuery()
        {
            var blogOne = new Blog
            {
                Title = "one",
                Category = "Ravens"
            };
            var blogTwo = new Blog
            {
                Title = "two",
                Category = "Rhinos"
            };
            var blogThree = new Blog
            {
                Title = "three",
                Category = "Rhinos"
            };

            using (var server = GetNewServer(port, path))
            {
                var store = new DocumentStore { Url = "http://localhost:" + port };
                store.Initialize();

                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<Blog>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                        .Where(x => x.Category == "Rhinos" && x.Title.Length == 3)
                        .ToArray();

                    var blogs = s.Advanced.LuceneQuery<Blog>()
                        .Where("Category:Rhinos AND Title.Length:3")
                        .ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("two", results[0].Title);
                    Assert.Equal("Rhinos", results[0].Category);
                }
            }
        }

        [Fact]
        public void CanPerformDynamicQueryUsingClientLuceneQuery()
        {
            var blogOne = new Blog
            {
                Title = "one",
                Category = "Ravens"
            };
            var blogTwo = new Blog
            {
                Title = "two",
                Category = "Rhinos"
            };
            var blogThree = new Blog
            {
                Title = "three",
                Category = "Rhinos"
            };

            using (var server = GetNewServer(port, path))
            {
                var store = new DocumentStore { Url = "http://localhost:" + port };
                store.Initialize();

                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Advanced.LuceneQuery<Blog>()
                        .Where("Title.Length:3 AND Category:Rhinos")
                        .WaitForNonStaleResultsAsOfNow().ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("two", results[0].Title);
                    Assert.Equal("Rhinos", results[0].Category);
                }
            }
        }

        [Fact]
        public void CanPerformProjectionUsingClientLinqQuery()
        {
            using (var server = GetNewServer(port, path))
            {
                var store = new DocumentStore { Url = "http://localhost:" + port };
                store.Initialize();

                var blogOne = new Blog
                {
                    Title = "one",
                    Category = "Ravens",
                    Tags = new Tag[] { 
                         new Tag() { Name = "tagOne"},
                         new Tag() { Name = "tagTwo"}
                    }
                };

                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<Blog>()
                        .Where(x => x.Title == "one" && x.Tags.Any(y => y.Name == "tagTwo"))
                        .Select(x => new
                        {
                            x.Category,
                            x.Title
                        })
                        .Single();

                    Assert.Equal("one", results.Title);
                    Assert.Equal("Ravens", results.Category);
                }
            }
        }

        [Fact]
        public void QueryForASpecificTypeDoesNotBringBackOtherTypes()
        {
            using (var server = GetNewServer(port, path))
            {
                var store = new DocumentStore { Url = "http://localhost:" + port };
                store.Initialize();

                using (var s = store.OpenSession())
                {
                    s.Store(new Tag());
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var results = s.Query<Blog>()
                        .Select(b => new { b.Category })
                        .ToArray();
                    Assert.Equal(0, results.Length);
                }
            }
        }

        [Fact]
        public void CanPerformLinqOrderByOnNumericField()
        {
            using (var server = GetNewServer(port, path))
            {
                var store = new DocumentStore { Url = "http://localhost:" + port };
                store.Initialize();

                var blogOne = new Blog
                {
                    SortWeight = 2
                };

                var blogTwo = new Blog
                {
                    SortWeight = 4
                };

                var blogThree = new Blog
                {                   
                    SortWeight = 1
                };

                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);                    
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var resultDescending = (from blog in s.Query<Blog>()
                                   orderby blog.SortWeight descending
                                   select blog).ToArray();

                    var resultAscending = (from blog in s.Query<Blog>()
                                           orderby blog.SortWeight ascending
                                           select blog).ToArray();

                    Assert.Equal(4, resultDescending[0].SortWeight);
                    Assert.Equal(2, resultDescending[1].SortWeight);
                    Assert.Equal(1, resultDescending[2].SortWeight);

                    Assert.Equal(1, resultAscending[0].SortWeight);
                    Assert.Equal(2, resultAscending[1].SortWeight);
                    Assert.Equal(4, resultAscending[2].SortWeight);                   

                }
            }
        }

        [Fact]
        public void CanPerformLinqOrderByOnTextField()
        {
            using (var server = GetNewServer(port, path))
            {
                var store = new DocumentStore { Url = "http://localhost:" + port };
                store.Initialize();

                var blogOne = new Blog
                {
                    Title = "aaaaa"
                };

                var blogTwo = new Blog
                {
                   Title = "ccccc"
                };

                var blogThree = new Blog
                {
                    Title = "bbbbb"
                };

                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var resultDescending = (from blog in s.Query<Blog>()
                                            orderby blog.Title descending
                                            select blog).ToArray();

                    var resultAscending = (from blog in s.Query<Blog>()
                                           orderby blog.Title ascending
                                           select blog).ToArray();

                    Assert.Equal("ccccc", resultDescending[0].Title);
                    Assert.Equal("bbbbb", resultDescending[1].Title);
                    Assert.Equal("aaaaa", resultDescending[2].Title);

                    Assert.Equal("aaaaa", resultAscending[0].Title);
                    Assert.Equal("bbbbb", resultAscending[1].Title);
                    Assert.Equal("ccccc", resultAscending[2].Title);

                }
            }
        }

        public class Blog
        {
            public User User
            {
                get;
                set;
            }

            public string Title
            {
                get;
                set;
            }

            public Tag[] Tags
            {
                get;
                set;
            }

            public int SortWeight { get; set; }

            public string Category
            {
                get;
                set;
            }
        }

        public class Tag
        {
            public string Name
            {
                get;
                set;
            }
        }

        public class User
        {
            public string Name
            {
                get;
                set;
            }
        }
    }
}
