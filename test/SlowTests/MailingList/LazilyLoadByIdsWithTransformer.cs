using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Transformers;
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
                    var items = session.Load<ItemsTransformer, Item>(new[] { "items/1-A", "items/2-A" });
                    Assert.Equal(1 * 3, items["items/1-A"].Position);
                    Assert.Equal(2 * 3, items["items/2-A"].Position);
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Advanced.Lazily.Load<ItemsTransformer, Item>(new[] { "items/1-A", "items/2-A" }).Value;
                    Assert.Equal(1 * 3, items["items/1-A"].Position);
                    Assert.Equal(2 * 3, items["items/2-A"].Position);
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
                    var items = session.Load<Item>(new[] { "items/1-A", "items/2-A" }, typeof(ItemsTransformer));
                    Assert.Equal(1 * 3, items["items/1-A"].Position);
                    Assert.Equal(2 * 3, items["items/2-A"].Position);
                }

                using (var session = store.OpenSession())
                {
                    var items = session.Advanced.Lazily.Load<Item>(new[] { "items/1-A", "items/2-A" }, typeof(ItemsTransformer)).Value;
                    Assert.Equal(1 * 3, items["items/1-A"].Position);
                    Assert.Equal(2 * 3, items["items/2-A"].Position);
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
