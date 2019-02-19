using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq.Indexing;
using Xunit;

namespace SlowTests.Tests.Indexes
{
    public class BoostingDuringIndexing : RavenTestBase
    {
        private class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        private class Account
        {
            public string Name { get; set; }
        }

        private class UsersByName : AbstractIndexCreationTask<User>
        {
            public UsersByName()
            {
                Map = users => from user in users
                               select new
                               {
                                   FirstName = user.FirstName.Boost(3),
                                   user.LastName
                               };
            }
        }

        private class UsersAndAccounts : AbstractMultiMapIndexCreationTask<UsersAndAccounts.Result>
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public UsersAndAccounts()
            {
                AddMap<User>(users =>
                             from user in users
                             select new { Name = user.FirstName }
                    );
                AddMap<Account>(accounts =>
                                from account in accounts
                                select new { account.Name }.Boost(3)
                    );
            }
        }

        [Fact(Skip = "RavenDB-6124")]
        public void CanBoostFullDocument()
        {
            using (var store = GetDocumentStore())
            {
                new UsersAndAccounts().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Oren",
                    });

                    session.Store(new Account()
                    {
                        Name = "Oren",
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Query<UsersAndAccounts.Result, UsersAndAccounts>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.Name == "Oren")
                        .As<object>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                    Assert.IsType<Account>(results[0]);
                    Assert.IsType<User>(results[1]);
                }
            }
        }

        [Fact]
        public void CanGetBoostedValues()
        {
            using (var store = GetDocumentStore())
            {
                new UsersByName().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        FirstName = "Oren",
                        LastName = "Eini"
                    });

                    session.Store(new User
                    {
                        FirstName = "Ayende",
                        LastName = "Rahien"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User, UsersByName>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Where(x => x.FirstName == "Ayende" || x.LastName == "Eini")
                        .ToList();

                    Assert.Equal("Ayende", users[0].FirstName);
                    Assert.Equal("Oren", users[1].FirstName);
                }
            }
        }
    }
}
