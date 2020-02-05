using System;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14363 : RavenTestBase
    {
        public RavenDB_14363(ITestOutputHelper output)
            : base(output)
        {
        }

        [Fact]
        public void Query_With_Where_Clause()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    newSession.Store(new User { Id = "users/1", Name = "John", LastName = "A" }, "users/1");
                    newSession.Store(new User { Id = "users/2", Name = "Jane", LastName = "B" }, "users/2");
                    newSession.Store(new User { Id = "users/3", Name = "Tarzan", LastName = "C" }, "users/3");
                    newSession.SaveChanges();

                    var queryGood = newSession.Query<User>()
                        .Where(x => x.Id.StartsWith("users/"))
                        .OrderBy(x => x.Name).ThenBy(x => x.LastName);

                    // without select work as expected
                    var queryBad = newSession.Query<User>()
                        .Where(x => x.Id.StartsWith("users/"))
                        .OrderBy(x => x.Name).ThenBy(x => x.LastName)
                        .Select(x => new UserWithoutId
                        {
                            Name = x.Name,
                            LastName = x.LastName
                        });

                    var strBad = queryBad.ToString();
                    var strGood = queryGood.ToString();

                    Assert.Equal("from 'Users' where startsWith(id(), $p0) order by Name, LastName select Name, LastName", strBad);
                    Assert.Equal("from 'Users' where startsWith(id(), $p0) order by Name, LastName", strGood);

                    var resultsBad = queryBad.ToList();
                    var resultsGood = queryGood.ToList();

                    Assert.Equal(3, resultsBad.Count);
                    Assert.Equal(3, resultsGood.Count);
                }
            }
        }

        private class User : UserWithoutId
        {
            public string Id { get; internal set; }
        }

        private class UserWithoutId
        {
            public string Name { get; internal set; }
            public string LastName { get; internal set; }
        }
    }
}
