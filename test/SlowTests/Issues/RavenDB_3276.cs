using System.Collections.Generic;
using System.Linq;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_3276 : RavenTestBase
    {
        public RavenDB_3276(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Dictionary_with_empty_string_as_key_should_fail_storing_in_db()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(
                        new TestEntity
                        {
                            Items = new Dictionary<string, string>
                            {
                                {"", "value for empty string"}
                            }
                        });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var entity = session.Load<TestEntity>("TestEntities/1-A");

                    Assert.NotNull(entity);
                    Assert.Equal(1, entity.Items.Count);
                    Assert.Equal("", entity.Items.Keys.First());
                    Assert.Equal("value for empty string", entity.Items[""]);
                }
            }
        }

        [Fact]
        public void Dictionary_with_empty_string_as_key_should_fail_bulk_insert()
        {
            using (var store = GetDocumentStore())
            {
                using (var bulkInsert = store.BulkInsert())
                {
                    bulkInsert.Store(new TestEntity
                    {
                        Items = new Dictionary<string, string>
                        {
                            {"", "value for empty string"}
                        }
                    });
                }

                using (var session = store.OpenSession())
                {
                    var entity = session.Load<TestEntity>("TestEntities/1-A");

                    Assert.NotNull(entity);
                    Assert.Equal(1, entity.Items.Count);
                    Assert.Equal("", entity.Items.Keys.First());
                    Assert.Equal("value for empty string", entity.Items[""]);
                }
            }
        }

        private class TestEntity
        {
            public string Id { get; set; }
            public Dictionary<string, string> Items { get; set; }
        }
    }
}
