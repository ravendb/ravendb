using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Bugs.Identifiers
{
    public class WithBase64Characters : RavenTestBase
    {
        public WithBase64Characters(ITestOutputHelper output) : base(output)
        {
        }

        private class Entity
        {
            public string Id { get; set; }
        }

        [Fact]
        public void Can_load_entity()
        {
            var specialId = "SHA1-UdVhzPmv0o+wUez+Jirt0OFBcUY=";

            using (var store = GetDocumentStore())
            {
                store.Initialize();

                using (var session = store.OpenSession())
                {
                    var entity = new Entity() { Id = specialId };
                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entity1 = session.Load<object>(specialId);
                    Assert.NotNull(entity1);
                }
            }
        }
    }
}
