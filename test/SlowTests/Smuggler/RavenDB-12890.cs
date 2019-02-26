using System;
using System.Reflection;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Xunit;

namespace SlowTests.Smuggler
{
    public class RavenDB_12890 : RavenTestBase
    {
        [Fact]
        public async Task CanImportDumpWithoutAttachment()
        {
            using (var stream = GetType().GetTypeInfo().Assembly.GetManifestResourceStream("SlowTests.Smuggler.Document_Without_Attachment_Stream.ravendbdump"))
            using (var store = GetDocumentStore())
            {
                Assert.NotNull(stream);

                var operation = await store.Smuggler.ImportAsync(new DatabaseSmugglerImportOptions(), stream);
                operation.WaitForCompletion<SmugglerResult>(TimeSpan.FromSeconds(30));

                using (var session = store.OpenSession())
                {
                    var o = session.Load<object>("test");
                    Assert.NotNull(o);

                    var attachmentNames = session.Advanced.Attachments.GetNames(o);
                    Assert.Equal(0, attachmentNames.Length);
                }
            }
        }
    }
}
