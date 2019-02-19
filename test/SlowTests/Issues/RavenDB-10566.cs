using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class CustomerMetadataAfterSaveChanges : RavenTestBase
    {
        private class Page
        {
            public string PageId;
        }

        private class Item
        {
            public Dictionary<string, Page> Trie = new Dictionary<string, Page>();
            public Dictionary<string, string> Items = new Dictionary<string, string>();
        }

        [Fact]
        public async Task CanUseObjectsInMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var v = new Item();
                    await session.StoreAsync(v, "items/first");
                    session.Advanced.GetMetadataFor(v).Add("Items", new Dictionary<string, string>
                    {
                        ["lang"] = "en"
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var v = await session.LoadAsync<Item>("items/first");
                    var metadata = session.Advanced.GetMetadataFor(v);
                    Assert.Equal("en", (((IDictionary<string, object>)metadata["Items"])["lang"]));
                }
            }
        }

        [Fact]
        public async Task CanPatchWithFilter()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item
                    {
                        Trie =
                        {
                            ["/"] = new Page{PageId = "home"},
                            ["/my-url"] = new Page{PageId = "my url"},
                        }
                    }, "items/first");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData("items/first", null,
                        new Raven.Client.Documents.Operations.PatchRequest
                        {
                            Script = @"
var self = this;
Object.keys(this.Trie).filter(function(key) {
    if(self.Trie[key].PageId === args.PageId) {
        delete self.Trie[key];
    }
});
",
                            Values =
                            {
                                ["PageId"] = "home",
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var item = await session.LoadAsync<Item>("items/first");
                    Assert.Equal(1, item.Trie.Count);
                    Assert.Equal("my url", item.Trie["/my-url"].PageId);
                }
            }
        }
        [Fact]
        public async Task CanPatchDictionaryUsingArgs()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Item(), "items/first");
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Defer(new PatchCommandData("items/first", null,
                        new Raven.Client.Documents.Operations.PatchRequest
                        {
                            Script = @"this.Items[args.Key] = args.Value;",
                            Values =
                            {
                                ["Key"] = "A",
                                ["Value"] = "B"
                            }
                        }, null));
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var v = new Item();
                    await session.StoreAsync(v, "items/first");
                    session.Advanced.GetMetadataFor(v).Add("Items", new Dictionary<string, string>
                    {
                        ["lang"] = "en"
                    });

                    await session.SaveChangesAsync();
                }
            }
        }

        [Fact]
        public async Task ShouldBeAvailable()
        {
            using (var store = GetDocumentStore())
            {
                string name = null;
                store.OnAfterSaveChanges += (object sender, AfterSaveChangesEventArgs e) =>
                {
                    name = (string)e.DocumentMetadata["Name"];
                };
                store.Initialize();

                using (var session = store.OpenAsyncSession())
                {
                    var user = new { Name = "Oren" };
                    await session.StoreAsync(user, "users/oren");
                    var metadata = session.Advanced.GetMetadataFor(user);
                    metadata.Add("Name", "FooBar");

                    await session.SaveChangesAsync();
                }

                Assert.Equal("FooBar", name);
            }
        }
    }
}
