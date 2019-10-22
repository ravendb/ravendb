// -----------------------------------------------------------------------
//  <copyright file="RavenDB_2233.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using FastTests;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3878 : RavenTestBase
    {
        public RavenDB_3878(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanHandle304InMultiGet()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Name = "user1"
                    };
                    session.Store(user, "users/1");
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    // fill HttpJsonRequestFactory cache with 'users/1'
                    session.Load<User>("users/1");
                }
                using (var session = store.OpenSession())
                {
                    // it respond with 304 
                    var load = session.Advanced.Lazily.Load<User>("users/1");
                    var user = load.Value;

                    Assert.NotNull(user);
                }
            }
        }
    }
}
