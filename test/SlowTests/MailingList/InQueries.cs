using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class InQueries : RavenTestBase
    {
        public InQueries(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Country { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void WhenQueryContainsQuestionMark(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void WhenQueryContainsOneElement(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void WhenElementcontainsCommas(Options options)
        {
            using (var store = GetDocumentStore(options))
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
        public void WhenElementContainsCommasInMiddleOfList()
        {
            using (var store = GetDocumentStore())
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

        /// <summary>
        /// This test requires us to qoute the search string and it test that we escape the comma within qoutes
        /// </summary>
        [Fact]
        public void WhenElementcontainsCommasInMiddleOfListWithWhiteSpaceBeforeTheComma()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Country = "Asia ,Japan"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var collection = session.Query<User>().Where(x => x.Country.In(new[] { "Korea", "Asia ,Japan", "China" })).ToList();

                    Assert.NotEmpty(collection);
                }
            }
        }
    }
}
