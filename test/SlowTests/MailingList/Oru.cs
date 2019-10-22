using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Oru : RavenTestBase
    {
        public Oru(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanSendOperationsWithSettingExplicitDbName()
        {
            using (var store = GetDocumentStore())
            using (var other = new DocumentStore
            {
                Urls = store.Urls,
            }.Initialize())
            {
                other.Operations.ForDatabase(store.Database).Send(
                    new GetAttachmentOperation("test", "test", Raven.Client.Documents.Attachments.AttachmentType.Document, null)
                    );
            }
        }
    }
}
