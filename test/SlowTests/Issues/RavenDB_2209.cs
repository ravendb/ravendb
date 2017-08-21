// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2209.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_2209 : RavenTestBase
    {
        [Fact]
        public void LazyLoadResultShouldBeUpToDateEvenIfAggressiveCacheIsEnabled()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = "users/1",
                        Name = "Arek"
                    });
                    session.SaveChanges();
                }

                using (store.AggressivelyCache())
                {
                    // make sure that object is cached
                    using (var session = store.OpenSession())
                    {
                        //store.Changes().Task.Result.WaitForAllPendingSubscriptions();

                        var users = session.Load<User>(new[] { "users/1" });

                        Assert.Equal("Arek", users["users/1"].Name);
                    }

                    using (var session = store.OpenSession())
                    {
                        var users = session.Advanced.Lazily.Load<User>(new[] { "users/1" });
                        session.Advanced.Lazily.Load<User>(new[] { "users/2" });

                        Assert.Equal("Arek", users.Value["users/1"].Name);
                    }

                    using (var session = store.OpenSession())
                    {
                        session.Store(new User
                        {
                            Id = "users/1",
                            Name = "Adam"
                        });
                        session.SaveChanges();
                    }

                    using (var session = store.OpenSession())
                    {
                        var users = session.Advanced.Lazily.Load<User>(new[] { "users/1" });
                        session.Advanced.Lazily.Load<User>(new[] { "users/2" });

                        Assert.Equal("Adam", users.Value["users/1"].Name);
                    }
                }
            }
        } 
    }
}
