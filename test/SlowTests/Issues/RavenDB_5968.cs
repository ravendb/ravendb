using FastTests;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_5968 : RavenTestBase
    {
        public RavenDB_5968(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EtagNullShouldOverrideDocument()
        {
            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "name1" }, null, "users/1");
                    s.SaveChanges();

                }
                using (var s = store.OpenSession())
                {
                    s.Store(new User { Name = "name2" }, null, "users/1");
                    s.SaveChanges();
                }
            }
        }

        [Fact]
        public void EtagNullShouldOverrideDocumentPut()
        {
            using (var store = GetDocumentStore())
            {
                using (var commands = store.Commands())
                {
                    commands.Put("users/1", null, new { Name = "Value1" }, null);
                    commands.Put("users/1", null, new { Name = "Value2" }, null);
                }

                using (var s = store.OpenSession())
                {
                    var user = s.Load<User>("users/1");
                    Assert.Equal("Value2", user.Name);
                }
            }
        }
    }
}
