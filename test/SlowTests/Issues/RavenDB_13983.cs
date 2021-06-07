using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Json;
using Sparrow.Server.Json.Sync;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13983 : RavenTestBase
    {
        public RavenDB_13983(ITestOutputHelper output) : base(output)
        {
        }

        public class Contact
        {
            public string Name { get; set; }
            public string Title { get; set; }
            public List<string> SomeList { get; set; } = new List<string>();
        }

        [Fact]
        public async Task SaveChangesShouldNotThrow()
        {
            using (var store = GetDocumentStore())
            {
                var ids = new List<string>();
                using (var session = store.OpenAsyncSession())
                {
                    for (var i = 0; i < 2; i++)
                    {
                        var entity = new Contact()
                        {
                            Name = $"{i}"
                        };
                        await session.StoreAsync(entity, $"Contact/{i}");
                        ids.Add($"Contact/{i}");
                        var ms = new MemoryStream(new byte[] { 1, 3, 3, 7 });
                        session.Advanced.Attachments.Store(entity, "initialAttachment", ms);
                    }

                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var entities = session.Query<Contact>().ToList();

                    foreach (var id in ids)
                    {
                        var attachment = new MemoryStream(new byte[] { 228 });
                        session.Advanced.Attachments.Store(id, "newAttachment", attachment);
                    }

                    session.SaveChanges();
                }
            }
        }

        [Fact]
        public void BjraShouldNotDisposeParentIfItsNotTheRoot()
        {
            const string jsonArr = "{\"b\":[1,2,3]}";
            var json = "{'Test': { 'a': ['a'] }, 'Fun': " + $"{jsonArr}" + "}";

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                var obj = context.Sync.ReadForDisk(new MemoryStream(Encoding.UTF8.GetBytes(json)), "test");
                obj.TryGet("Test", out BlittableJsonReaderObject a1);
                obj.TryGet("Fun", out BlittableJsonReaderObject a2);

                a1.TryGet("a", out BlittableJsonReaderArray arr);
                a1.Dispose();

                Assert.Equal(jsonArr, a2.ToString());
            }
        }
    }
}
