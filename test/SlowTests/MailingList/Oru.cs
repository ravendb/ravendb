using System;
using System.Collections.Generic;
using System.Text;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Attachments;
using Xunit;

namespace SlowTests.MailingList
{
    public class Oru : RavenTestBase
    {
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
