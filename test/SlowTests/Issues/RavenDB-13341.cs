using System.Threading.Tasks;
using FastTests;
using Raven.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13341 : RavenTestBase
    {
        [Fact]
        public async Task CanGetRevisionsByDate()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                var documentDatabase = (await GetDocumentDatabaseInstanceFor(store));
                var documentsStorage = documentDatabase.DocumentsStorage;

                using (documentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var attachmentStorage = documentDatabase.DocumentsStorage.AttachmentsStorage;
                    var documents = documentsStorage.GetDocumentsFrom(context, etag: 0);
                    foreach (var document in documents)
                    {
                        VerifyAttachments();

                        void VerifyAttachments()
                        {
                            if (document.Data.TryGet(Constants.Documents.Metadata.Key, out BlittableJsonReaderObject metadata) == false)
                                return;

                            if (metadata.TryGet(Constants.Documents.Metadata.Attachments, out BlittableJsonReaderArray attachments) == false)
                                return;

                            foreach (BlittableJsonReaderObject attachmentFromDocument in attachments)
                            {
                                attachmentFromDocument.TryGet(nameof(AttachmentName.Name), out string attachmentName);
                                var attachment = attachmentStorage.GetAttachment(context, document.Id, attachmentName, AttachmentType.Document, null);
                                Assert.True(document.Etag > attachment.Etag);

                                var conflictStatus = ChangeVectorUtils.GetConflictStatus(document.ChangeVector, attachment.ChangeVector);
                                Assert.Equal(ConflictStatus.Update, conflictStatus);
                            }
                        }
                    }
                }
            }
        }
    }
}
