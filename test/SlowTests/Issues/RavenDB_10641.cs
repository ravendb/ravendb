using System.Collections.Generic;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10641 : RavenTestBase
    {
        public RavenDB_10641(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanEditObjectsInMetadata()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var v = new Document();
                    await session.StoreAsync(v, "items/first");
                    session.Advanced.GetMetadataFor(v).Add("Items", new Dictionary<string, string>
                    {
                        ["lang"] = "en"
                    });

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var v = await session.LoadAsync<Document>("items/first");
                    var metadata = session.Advanced.GetMetadataFor(v).GetObject("Items");
                    metadata["lang"] = "sv";

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var v = await session.LoadAsync<Document>("items/first");
                    var metadata = session.Advanced.GetMetadataFor(v);
                    metadata["test"] = "123";

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var v = await session.LoadAsync<Document>("items/first");
                    var metadata = session.Advanced.GetMetadataFor(v);

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var v = await session.LoadAsync<Document>("items/first");
                    var metadata = session.Advanced.GetMetadataFor(v);
                    Assert.Equal("sv", ((IDictionary<string, object>)metadata["Items"])["lang"]);
                    Assert.Equal("123", (string)metadata["test"]);
                }
            }
        }

        private class Document
        {
        }
    }
}
