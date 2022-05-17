using Tests.Infrastructure;
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanBoostFullDocument(Options options)
        {
            using (var store = GetDocumentStore(options))
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

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanGetBoostedValues(Options options)
        {
            using (var store = GetDocumentStore(options))
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

                Indexes.WaitForIndexing(store);

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
