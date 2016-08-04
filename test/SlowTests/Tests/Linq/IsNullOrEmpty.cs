using System.Threading.Tasks;
using FastTests;
using Xunit;
using System.Linq;

namespace SlowTests.Tests.Linq
{
    public class IsNullOrEmpty : RavenTestBase
    {
        private class TestDoc
        {
            public string SomeProperty { get; set; }
        }

        [Fact]
        public async Task IsNullOrEmptyEqTrue()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2, session.Query<TestDoc>().Count(p => string.IsNullOrEmpty(p.SomeProperty)));
                }

                WaitForUserToContinueTheTest(store);
            }
        }

        [Fact]
        public async Task IsNullOrEmptyEqFalse()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TestDoc>().Count(p => string.IsNullOrEmpty(p.SomeProperty) == false));
                }
            }
        }

        [Fact]
        public async Task IsNullOrEmptyNegated()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TestDoc>().Count(p => !string.IsNullOrEmpty(p.SomeProperty)));
                }
            }
        }

        [Fact]
        public async Task WithAny()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(1, session.Query<TestDoc>().Count(p => p.SomeProperty.Any()));
                }
            }
        }

        [Fact]
        public async Task WithAnyEqFalse()
        {
            using (var store = await GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "Has some content" });
                    session.Store(new TestDoc { SomeProperty = "" });
                    session.Store(new TestDoc { SomeProperty = null });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.Equal(2, session.Query<TestDoc>().Count(p => p.SomeProperty.Any() == false));
                }
            }
        }
    }
}
