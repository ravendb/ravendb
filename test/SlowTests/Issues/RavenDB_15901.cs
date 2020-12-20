using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15901 : RavenTestBase
    {
        public RavenDB_15901(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
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

                WaitForIndexing(store);

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

        private class User
        {
            public string FirstName { get; set; }
            public string LastName { get; set; }
        }

        private class Account
        {
            public string Name { get; set; }
        }

        private class UsersByName : AbstractJavaScriptIndexCreationTask
        {
            public UsersByName()
            {
                Maps = new HashSet<string>
                {
                    @"map('Users', function (u) { return { FirstName: boost(u.FirstName, 3), LastName: u.LastName }; })",
                };
            }
        }

        private class UsersAndAccounts : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Name { get; set; }
            }

            public UsersAndAccounts()
            {
                Maps = new HashSet<string>
                {
                    @"map('Users', function (u) { return { Name: u.FirstName }; })",
                    @"map('Accounts', function (a) { return boost({ Name: a.Name }, 3) })"
                };
            }
        }
    }
}
