using System.Linq;
using FastTests;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class LazilyLoadByIdsWithTransformer : RavenTestBase
    {
        [Fact]
        public void WithTransformer()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new ItemsTransformer());

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Position = 1 });
                    session.Store(new Item { Position = 2 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Load<ItemsTransformer, Item>(new[] { "items/1", "items/2" });
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Advanced.Lazily.Load<ItemsTransformer, Item>(new[] { "items/1", "items/2" }).Value;
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }
            }
        }

        [Fact]
        public void WithTransformer2()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteTransformer(new ItemsTransformer());

                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Position = 1 });
                    session.Store(new Item { Position = 2 });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Load<Item>(new[] { "items/1", "items/2" }, typeof(ItemsTransformer));
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Advanced.Lazily.Load<Item>(new[] { "items/1", "items/2" }, typeof(ItemsTransformer)).Value;
                    Assert.Equal(1 * 3, items[0].Position);
                    Assert.Equal(2 * 3, items[1].Position);
                }
            }
        }

        private class Item
        {
            public int Position { get; set; }
        }

        private class ItemsTransformer : AbstractTransformerCreationTask<Item>
        {
            public ItemsTransformer()
            {
                TransformResults = docs => docs.Select(doc => new { Position = doc.Position * 3 });
            }
        }
    }
}
