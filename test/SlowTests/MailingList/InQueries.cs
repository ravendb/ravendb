using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class InQueries : RavenTestBase
    {
        private class User
        {
            public string Country { get; set; }
        }

        [Fact]
        public async Task WhenQueryContainsQuestionMark()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Country = "Asia?"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var collection = session.Query<User>().Where(x => x.Country.In("Asia?", "Israel*")).ToList();
                    Assert.NotEmpty(collection);
                }
            }
        }

        [Fact]
        public async Task WhenQueryContainsOneElement()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Country = "Asia"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<User>()
                        .Where(x => x.Country.In("Asia"))
                        .ToList());
                }
            }
        }

        [Fact]
        public async Task WhenElementcontainsCommas()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Country = "Asia,Japan"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var collection = session.Query<User>().Where(x => x.Country.In("Asia,Japan")).ToList();

                    Assert.NotEmpty(collection);
                }
            }
        }

        [Fact]
        public async Task WhenElementcontainsCommasInMiddleOfList()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Country = "Asia,Japan"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var collection = session.Query<User>().Where(x => x.Country.In(new[] { "Korea", "Asia,Japan", "China" })).ToList();

                    Assert.NotEmpty(collection);
                }
            }
        }
    }
}
