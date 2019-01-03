using System.Linq;
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

        private class ProjectedItem
        {
            public bool Before { get; set; }

            public bool After { get; set; }
        }

        [Fact]
        public void CanUseToDocumentConversionEvents()
        {
            using (var store = GetDocumentStore())
            {
                store.OnBeforeConversionToDocument += (sender, args) =>
                {
                    if (args.Entity is Item item)
                        item.Before = true;
                };

                store.OnAfterConversionToDocument += (sender, args) =>
                {
                    if (args.Entity is Item item)
                    {
                        if (args.Document.Modifications == null)
                            args.Document.Modifications = new DynamicJsonValue();

                        args.Document.Modifications["After"] = true;
                        args.Document = args.Session.Context.ReadObject(args.Document, args.Id);

                        item.After = true;
                    }
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.SaveChanges();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);

                    session.SaveChanges();

                    Assert.Equal(1, session.Advanced.NumberOfRequests);
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

        [Fact]
        public void CanUseToEntityConversionEvents()
        {
            using (var store = GetDocumentStore())
            {
                store.OnBeforeConversionToEntity += (sender, args) =>
                {
                    var document = args.Document;
                    if (document.Modifications == null)
                        document.Modifications = new DynamicJsonValue();

                    document.Modifications["Before"] = true;
                    args.Document = args.Session.Context.ReadObject(document, args.Id);
                };

                store.OnAfterConversionToEntity += (sender, args) =>
                {
                    if (args.Entity is Item item)
                        item.After = true;
                    if (args.Entity is ProjectedItem projectedItem)
                        projectedItem.After = true;
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new Item(), "items/1");
                    session.Store(new Item(), "items/2");
                    session.SaveChanges();
                }

                // load
                using (var session = store.OpenSession())
                {
                    var item = session.Load<Item>("items/1");

                    Assert.NotNull(item);
                    Assert.True(item.Before);
                    Assert.True(item.After);
                }

                // queries
                using (var session = store.OpenSession())
                {
                    var items = session.Query<Item>().ToList();

                    Assert.Equal(2, items.Count);

                    foreach (var item in items)
                    {
                        Assert.True(item.Before);
                        Assert.True(item.After);
                    }
                }

                // projections in queries
                using (var session = store.OpenSession())
                {
                    var items = session
                        .Query<Item>()
                        .Select(x => new ProjectedItem
                        {
                            After = x.After,
                            Before = x.Before
                        })
                        .ToList();

                    Assert.Equal(2, items.Count);

                    foreach (var item in items)
                    {
                        Assert.True(item.Before);
                        Assert.True(item.After);
                    }
                }
            }
        }
    }
}
