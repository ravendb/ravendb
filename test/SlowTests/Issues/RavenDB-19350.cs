using FastTests;
using Xunit.Abstractions;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_19350 : RavenTestBase
    {
        public RavenDB_19350(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanStreamProjectionWithMultipleResultsPerDocument()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                var item = new Item
                {
                    Inners = new[]
                    {
                        new Item{N= "a"},
                        new Item{N= "b"},
                    }
                };
                session.Store(item);
                session.SaveChanges();
            }

            using(var session = store.OpenSession())
            {
                var q = session.Advanced.RawQuery<object>(@"
declare function project(o) {
    return o.Inners;
}
from Items as o
select project(o)");

                using(var s = session.Advanced.Stream(q))
                {
                    while (s.MoveNext())
                    {

                    }
                }
            }
        }

        public class Item
        {
            public string N;
            public object[] Inners;
        }
    }
}
