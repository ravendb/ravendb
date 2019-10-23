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
    public class RavenDB_2233 : RavenTestBase
    {
        public RavenDB_2233(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanMultipleQuery()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1"
                    }, "users/1");
                    session.Store(new User
                    {
                        Name = "user1"
                    }, "users/2");
                    session.Store(new User
                    {
                        Name = "user1"
                    }, "users/3");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var results = session.Load<User>(new[] { "users/3", "users/2", "users/1", "users/999", "users/2" });
                    Assert.Equal(4, results.Count);
                    Assert.Equal("users/3", session.Advanced.GetMetadataFor(results["users/3"])["@id"]);
                    Assert.Equal("users/2", session.Advanced.GetMetadataFor(results["users/2"])["@id"]);
                    Assert.Equal("users/1", session.Advanced.GetMetadataFor(results["users/1"])["@id"]);
                    Assert.Equal(null, results["users/999"]);
                    Assert.Equal("users/2", session.Advanced.GetMetadataFor(results["users/2"])["@id"]);
                }
            }
        }
    }
}
