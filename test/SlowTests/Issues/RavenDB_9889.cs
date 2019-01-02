using FastTests;
using Sparrow.Json.Parsing;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_9889 : RavenTestBase
    {
        private class Item
        {
            public string Id { get; set; }

            public bool Before { get; set; }

            public bool After { get; set; }
        }

        [Fact]
        public void CanUseConversionEvents()
        {
            using (var store = GetDocumentStore())
            {
                store.OnBeforeConversionToEntity += (sender, args) =>
                {
                    var document = args.Document;
                    if (document.Modifications == null)
                        document.Modifications = new DynamicJsonValue();

                    document.Modifications["Before"] = true;
                };

                store.OnAfterConversionToEntity += (sender, args) =>
                {
                    if (args.Entity is Item item)
                        item.After = true;
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");
                    
                    Assert.NotNull(item);
                    Assert.True(item.Before);
                    Assert.True(item.After);
                }
            }
        }
    }
}
