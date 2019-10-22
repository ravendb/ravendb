using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RDBC_76 : RavenTestBase
    {
        public RDBC_76(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void EmptyEnumerableInContainsAnyShouldYieldNoResults()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Item
                    {
                        Value = 10,
                        Names = new[] { "Bob", "John" }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Query<Item>()
                        .Where(x => x.Names.ContainsAny(Enumerable.Empty<string>()))
                        .ToList();

                    Assert.Equal(0, items.Count);

                    items = session.Query<Item>()
                        .Where(x => x.Names.ContainsAny(new[] { "Bob" }))
                        .ToList();

                    Assert.Equal(1, items.Count);

                    items = session.Query<Item>()
                        .Where(x => x.Names.ContainsAny(Enumerable.Empty<string>()) && x.Value == 10)
                        .ToList();

                    Assert.Equal(0, items.Count);

                    items = session.Query<Item>()
                        .Where(x => x.Names.ContainsAny(Enumerable.Empty<string>()) || x.Value == 10)
                        .ToList();

                    Assert.Equal(1, items.Count);
                }
            }
        }

        private class Item
        {
            public int Value { get; set; }

            public string[] Names { get; set; }
        }
    }
}
