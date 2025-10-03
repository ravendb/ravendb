using System.IO;
using Raven.Client.Documents.Commands.Batches;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14041 : ClusterTestBase
    {
        public RavenDB_14041(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments | RavenTestCategory.ClientApi)]
        public void CanUpdateAttachmentUsingPutAttachmentCommandDataWithExpectedChangeVector()
        {
            using (var store = GetDocumentStore())
            {
                const string documentId = "cats/1-A";
                const string name = "bruhhh";
                const string contentType = "stuff";

                using (var session = store.OpenSession())
                {
                    session.Store(new User(), documentId);

                    using var stream = new MemoryStream(new byte[] { 1, 2, 3, 4, 5 });
                    session.Advanced.Attachments.Store(documentId, name, stream, contentType);

                    session.SaveChanges();
                }

                // update attachment
                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(documentId, name);
                    using var stream = new MemoryStream(new byte[] { 6, 7, 8, 9, 10 });

                    // should not throw a concurrency exception
                    var cmd = new PutAttachmentCommandData(documentId, name, stream, contentType, changeVector: attachment.Details.ChangeVector);
                    session.Advanced.Defer(cmd);
                    session.SaveChanges();
                }

            }
        }
    }

}
