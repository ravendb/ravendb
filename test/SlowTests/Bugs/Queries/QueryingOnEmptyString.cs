using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Queries
{
    public class QueryingOnEmptyString : RavenTestBase
    {
        public QueryingOnEmptyString(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldNotSelectAllDocs()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Ayende" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Empty(session.Query<User>()
                        .Where(x => x.Name == string.Empty)
                        .ToList());
                }
            }
        }

        [Fact]
        public void CanFindByemptyStringMatch()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<User>()
                        .Where(x => x.Name == string.Empty)
                        .ToList());
                }
            }
        }

        private class User
        {
            public string Name { get; set; }
        }
    }
}
