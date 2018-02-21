using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class CustomerMetadataAfterSaveChanges : RavenTestBase
    {
        public class Item
        {
            public Dictionary<string, string> Items = new Dictionary<string, string>();
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
                    var item = await session.LoadAsync<Item>("items/first");
                    Assert.Equal("B", item.Items["A"]);
                }
            }
        }

        [Fact]
        public async Task ShouldBeAvailable()
        {
            using (var store = GetDocumentStore())
            {
                string name = null;
                store.OnAfterSaveChanges += (object sender, AfterSaveChangesEventArgs e) => {
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
