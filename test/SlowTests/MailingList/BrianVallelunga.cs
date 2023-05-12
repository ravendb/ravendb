// -----------------------------------------------------------------------
//  <copyright file="BrianVallelunga.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class BrianVallelunga : RavenTestBase
    {
        public BrianVallelunga(ITestOutputHelper output) : base(output)
        {
        }

        private class Profile
        {
            public string Name { get; set; }
            public string FavoriteColor { get; set; }
        }

        private class Account
        {
            public string Id { get; set; }
            public Profile Profile { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanProjectAndSort(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   orderby a.Profile.Name
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanSort(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }

        [Fact]
        public void CanProjectAndSort_Remote()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   orderby a.Profile.Name
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }

        [Fact]
        public void CanSort_Remote()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Account
                    {
                        Profile = new Profile
                        {
                            FavoriteColor = "Red",
                            Name = "Yo"
                        }
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var results = (from a in session.Query<Account>()
                                   .Customize(x => x.WaitForNonStaleResults())
                                   select new { a.Id, a.Profile.Name, a.Profile.FavoriteColor }).ToArray();


                    Assert.Equal("Red", results[0].FavoriteColor);
                }
            }
        }
    }
}
