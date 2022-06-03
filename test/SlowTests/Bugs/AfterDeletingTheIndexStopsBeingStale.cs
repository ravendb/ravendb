//-----------------------------------------------------------------------
// <copyright file="AfterDeletingTheIndexStopsBeingStale.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs
{
    public class AfterDeletingTheIndexStopsBeingStale : RavenTestBase
    {
        public AfterDeletingTheIndexStopsBeingStale(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Deletion(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 3; i++)
                    {
                        session.Store(new User { Name = "Ayende" });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(20))).ToList();
                    Assert.NotEmpty(users);
                    foreach (var user in users)
                    {
                        session.Delete(user);
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var users = session.Query<User>().Customize(x => x.WaitForNonStaleResults(TimeSpan.FromMinutes(20))).ToList();
                    Assert.Empty(users);
                }
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }
    }
}
