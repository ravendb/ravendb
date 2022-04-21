using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class AllPropertiesIndex : RavenTestBase
    {
        public AllPropertiesIndex(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        private class Users_AllProperties : AbstractIndexCreationTask<User, Users_AllProperties.Result>
        {
            public class Result
            {
                public string Query { get; set; }
            }

            public Users_AllProperties()
            {
                Map = users =>
                      from user in users
                      select new
                      {
                          Query = AsJson(user).Select(x => x.Value)
                      };
                Index(x => x.Query, FieldIndexing.Search);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void CanSearchOnAllProperties(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                new Users_AllProperties().Execute(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new User
                    {
                        FirstName = "Ayende",
                        LastName = "Rahien"
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    Assert.NotEmpty(s.Query<Users_AllProperties.Result, Users_AllProperties>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .Where(x => x.Query == "Ayende")
                                        .As<User>()
                                        .ToList());

                    Assert.NotEmpty(s.Query<Users_AllProperties.Result, Users_AllProperties>()
                                        .Customize(x => x.WaitForNonStaleResults())
                                        .Where(x => x.Query == "Ayende")
                                        .As<User>()
                                        .ToList());

                }
            }

        }

    }
}
