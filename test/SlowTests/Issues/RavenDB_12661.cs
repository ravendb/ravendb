using System.IO;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12661 : RavenTestBase
    {
        [Fact]
        public async Task Can_delete_simple_attachment()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenAsyncSession())
                {
                    var document = new TestDocument { Id = "docs/1" };
                    await session.StoreAsync(document);

                    using (StoreTestFile(session, document, "test.txt", "test"))
                    {
                        await session.SaveChangesAsync();
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    var document = await session.LoadAsync<TestDocument>("docs/1");

                    session.Advanced.Attachments.Delete(document, "test.txt");

                    await session.SaveChangesAsync();
                }
            }
        }

        private MemoryStream StoreTestFile<T>(IAsyncDocumentSession session, T entity, string filename, string content)
        {
            var stream = new MemoryStream();
            session.Advanced.Attachments.Store(entity, filename, stream, "text/plain");
            return stream;
        }
    }
}
