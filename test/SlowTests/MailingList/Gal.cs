using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class Gal : RavenTestBase
    {
        private class BlogPost
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [Fact]
        public void UsingInQuery()
        {
            var id1 = Guid.Parse("00000000-0000-0000-0000-000000000001").ToString();
            var id2 = Guid.Parse("00000000-0000-0000-0000-000000000002").ToString();
            var id3 = Guid.Parse("00000000-0000-0000-0000-000000000003").ToString();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Id = id1,
                        Name = "one",
                    });
                    session.Store(new BlogPost
                    {
                        Id = id2,
                        Name = "two"
                    });
                    session.Store(new BlogPost
                    {
                        Id = id3,
                        Name = "three"
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);
                using (var session = store.OpenSession())
                {
                    var myGroupOfIds = new[] { id1, id3 };

                    var goodResult = session.Query<BlogPost>()
                        .Where(i => i.Id.In(myGroupOfIds)).ToArray();

                    Assert.Equal(2, goodResult.Select(i => i.Name).ToArray().Length);

                    QueryStatistics stats;
                    var badResult = session.Query<BlogPost>()
                        .Statistics(out stats)
                        .Where(i => i.Id.In(myGroupOfIds)).Select(i => new { i.Name }).ToArray();

                    Assert.Equal(2, badResult.Select(i => i.Name).ToArray().Length);

                }
            }
        }
    }
}
