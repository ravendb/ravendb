using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
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
			Thread.Sleep(100);
			Directory.Delete(path, true);
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
                    var results = s.DynamicQuery<Blog>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                        .Where(x => x.Category == "Rhinos" && x.Title.Length == 3)
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
                    var results = s.DynamicLuceneQuery<Blog>()
                        .Where("Title.Length:3 AND Category:Rhinos")
                        .WaitForNonStaleResultsAsOfNow().ToArray();

                    Assert.Equal(1, results.Length);
                    Assert.Equal("two", results[0].Title);
                    Assert.Equal("Rhinos", results[0].Category);
                }
            }
        }

        public class Blog
        {
            public string Title
            {
                get;
                set;
            }

            public string Category
            {
                get;
                set;
            }
        }
    }
}
