using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12266 : RavenTestBase
    {
        [Fact]
        public void Test()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item()
                    {
                        Numbers = new int[] { 1, 2 }
                    });

                    session.Store(new Item()
                    {
                        Numbers = new int[] { 3 }
                    });

                    session.SaveChanges();

                    var results = session.Query<Item>().OrderBy(x => x.Numbers).ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }

        private class Item
        {
            public string Id { get; set; }

            public int[] Numbers { get; set; }
        }
    }
}
