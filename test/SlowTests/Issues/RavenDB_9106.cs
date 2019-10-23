using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_9106: RavenTestBase
    {
        public RavenDB_9106(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CollectionQueryStartsWithOnIdsShouldNotYieldResultsFromAnotherCollection()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new GoodPoco { Name = "Good"});
                session.Store(new EvilPoco { Name = "Evil" }, "GoodPocos/173");
                session.SaveChanges();
                WaitForUserToContinueTheTest(store);
                var res = session.Advanced.RawQuery<dynamic>("from GoodPocos where StartsWith(id(),'GoodPocos/')").ToList();
                Assert.Single(res);
                var goodPoco = res.First();
                Assert.Equal("Good", goodPoco.Name);
            }
        }

        [Fact]
        public void CollectionQueryOnIdsShouldNotYieldResultsFromAnotherCollection()
        {
            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {
                session.Store(new GoodPoco { Name = "Good" });
                session.Store(new EvilPoco { Name = "Evil" }, "GoodPocos/173");
                session.SaveChanges();
                WaitForUserToContinueTheTest(store);
                var res = session.Advanced.RawQuery<dynamic>("from GoodPocos where id() ='GoodPocos/173'").ToList();
                Assert.Empty(res);
            }
        }
        private class EvilPoco
        {
            public string Name { get; set; }
        }

        private class GoodPoco
        {
            public string Name { get; set; }
        }
    }
}
