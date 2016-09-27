using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Client.Linq;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient
{
    public class Query : RavenTestBase
    {
        [Fact]
        public void Query_Simple()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User { Name = "John" }, "users/1");
                    newSession.Store(new User { Name = "Jane" }, "users/2");
                    newSession.Store(new User { Name = "Tarzan" }, "users/3");
                    newSession.SaveChanges();

                    var queryResult = newSession.Query<User>()
                        .ToList();

                    Assert.Equal(queryResult.Count, 3);
                }
            }
        }

        [Fact]
        public void Query_With_Where_Clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenNewSession())
                {
                    newSession.Store(new User { Name = "John" }, "users/1");
                    newSession.Store(new User { Name = "Jane" }, "users/2");
                    newSession.Store(new User { Name = "Tarzan" }, "users/3");
                    newSession.SaveChanges();

                    var queryResult = newSession.Query<User>()
                        .Where(x => x.Name.StartsWith("J"))
                        .ToList();

                    var queryResult2 = newSession.Query<User>()
                        .Where(x => x.Name.Equals("Tarzan"))
                        .ToList();

                    Assert.Equal(queryResult.Count, 2);
                    Assert.Equal(queryResult2.Count, 1);
                }
            }
        }
    }
}

