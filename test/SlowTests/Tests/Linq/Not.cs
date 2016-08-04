using System.Threading.Tasks;
using FastTests;
using Xunit;
using System.Linq;

namespace SlowTests.Tests.Linq
{
    public class CanQueryWithSavedKeywords : RavenTestBase
    {
        private class TestDoc
        {
            public string SomeProperty { get; set; }
        }

        [Fact]
        public async Task CanQueryWithNot()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "NOT" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Query<TestDoc>().FirstOrDefault(doc => doc.SomeProperty == "NOT"));
                }
            }
        }

        [Fact]
        public async Task CanQueryWithOr()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "OR" });
                    session.SaveChanges();
                }

                WaitForUserToContinueTheTest(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Query<TestDoc>().FirstOrDefault(doc => doc.SomeProperty == "OR"));
                }
            }
        }

        [Fact]
        public async Task CanQueryWithAnd()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "AND" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Query<TestDoc>().FirstOrDefault(doc => doc.SomeProperty == "AND"));
                }
            }
        }
    }
}
