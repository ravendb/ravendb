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
        public void CanQueryWithNot()
        {
            using (var store = GetDocumentStore())
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
        public void CanQueryWithOr()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new TestDoc { SomeProperty = "OR" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotNull(session.Query<TestDoc>()
                        .Customize(x=>x.WaitForNonStaleResults())
                        .FirstOrDefault(doc => doc.SomeProperty == "OR"));
                }
            }
        }

        [Fact]
        public void CanQueryWithAnd()
        {
            using (var store = GetDocumentStore())
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
