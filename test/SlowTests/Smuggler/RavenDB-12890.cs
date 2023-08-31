using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Smuggler;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Smuggler
{
    public class RavenDB_12890 : RavenTestBase
    {
        public RavenDB_12890(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanImportDumpWithoutAttachment()
        {
            using (var stream = GetType().Assembly.GetManifestResourceStream("SlowTests.Smuggler.Data.Document_Without_Attachment_Stream.ravendbdump"))
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
