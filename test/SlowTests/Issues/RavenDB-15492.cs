using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15492 : RavenTestBase
    {
        public RavenDB_15492(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WillCallOnBeforeDeleteWhenCallingDeleteById()
        {
            using var store = GetDocumentStore();

            using (var s = store.OpenSession())
            {
                bool called = false;
                s.Advanced.OnBeforeDelete += (sender, args) => called = args.DocumentId == "users/1";
                s.Delete("users/1");
                s.SaveChanges();
                Assert.True(called);
            }
            
        }
    }
}
