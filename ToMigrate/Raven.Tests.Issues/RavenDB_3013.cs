using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Json.Linq;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_3013 : RavenTestBase
    {
        [Fact]
        public void CanPersistLinqWhereIEnumerable()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {					
                    var entity = new TestEntity
                    {
                        Id = 1,
                        Property = new[] { "one", "two" }.Where(s => s.Length > 1)
                    };

                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadedEntity = session.Load<TestEntity>(1);
                    Assert.Contains("one", loadedEntity.Property);
                    Assert.Contains("two", loadedEntity.Property);
                }
            }
        }

        [Fact]
        public void CanPersistLinqSelectIEnumerable()
        {
            using (var store = NewDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var entity = new TestEntity2
                    {
                        Id = 1,
                        Property = new[] { 1, 2 }.Select(x => x - 1)
                    };

                    session.Store(entity);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var loadedEntity = session.Load<TestEntity2>(1);
                    var array = loadedEntity.Property.ToList();

                    Assert.Equal(0, array[0]);
                    Assert.Equal(1, array[1]);
                }
            }
        }

        public class TestEntity
        {
            public int Id { get; set; }
            public IEnumerable<string> Property { get; set; }
        }


        public class TestEntity2
        {
            public int Id { get; set; }
            public IEnumerable<int> Property { get; set; }
        }
    }
}
