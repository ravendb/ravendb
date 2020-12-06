using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15983 : RavenTestBase
    {
        public RavenDB_15983(ITestOutputHelper output) : base(output)
        {
        }

        private class Item
        {

        }

        private class Index : AbstractIndexCreationTask<Item>
        {
            public Index()
            {
                Map = items =>
                    from item in items
                    select new
                    {
                        _ = new[]
                        {
                            CreateField("foo", "a"),
                            CreateField("foo", "b"),
                        }
                    };
            }
        }

        [Fact]
        public void CanIndexMultipleFieldsWithSameNameUsingCreateField()
        {
            using var store = GetDocumentStore();
            new Index().Execute(store);
            using (var session = store.OpenSession())
            {
                session.Store(new Item());
                session.SaveChanges();
            }
            WaitForIndexing(store);

            using (var session = store.OpenSession())
            {
                var c = session.Advanced.RawQuery<object>("from index 'Index' where foo = 'a'").Count();
                Assert.Equal(1, c);
                c = session.Advanced.RawQuery<object>("from index 'Index' where foo = 'b'").Count();
                Assert.Equal(1, c);
            }
        }
    }
}
