using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3013 : RavenTestBase
    {
        public RavenDB_3013(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanPersistLinqWhereIEnumerable()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var entity = new TestEntity
                    {
                        Property = new[] { "one", "two" }.Where(s => s.Length > 1)
                    };

                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadedEntity = session.Load<TestEntity>("TestEntities/1-A");
                    Assert.Contains("one", loadedEntity.Property);
                    Assert.Contains("two", loadedEntity.Property);
                }
            }
        }

        [Fact]
        public void CanPersistLinqSelectIEnumerable()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var entity = new TestEntity2
                    {
                        Property = new[] { 1, 2 }.Select(x => x - 1)
                    };

                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadedEntity = session.Load<TestEntity2>("TestEntity2s/1-A");
                    var array = loadedEntity.Property.ToList();
                    Assert.Equal(0, array[0]);
                    Assert.Equal(1, array[1]);
                }
            }
        }

        private class TestEntity
        {
            public string Id { get; set; }
            public IEnumerable<string> Property { get; set; }
        }

        private class TestEntity2
        {
            public string Id { get; set; }
            public IEnumerable<int> Property { get; set; }
        }
    }
}
