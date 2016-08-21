using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Xunit;

namespace SlowTests.MailingList
{
    public class AllPropertiesIndex : RavenTestBase
    {
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
                          Query = AsDocument(user).Select(x => x.Value)
                      };
                Index(x => x.Query, FieldIndexing.Analyzed);
            }
        }

        [Fact]
        public async Task CanSearchOnAllProperties()
        {
            using (var store = await GetDocumentStore())
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
