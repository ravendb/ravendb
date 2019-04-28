using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Exceptions;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.Json;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13320 : RavenTestBase
    {
        [Fact]
        public async Task FailingToPutAttachmentShouldFailTheMerger()
        {
            using (var store = GetDocumentStore())
            {
                var dbId1 = new Guid("00000000-48c4-421e-9466-000000000000");
                var name = "profile.png";
                await SetDatabaseId(store, dbId1);
                string cv1 = "A:1-AAAAAMRIHkKUZgAAAAAAAA";
                string cv2 = "A:2-AAAAAMRIHkKUZgAAAAAAAA";

                var id = "users/1";
                using (var profileStream = new MemoryStream(new byte[] {1, 2, 3}))
                {
                    using (var session = store.OpenSession())
                    {
                        var user = new User
                        {
                            Name = "Tal"
                        };
                        session.Store(user, id);                       
                        session.SaveChanges();
                        store.Operations.Send(new PutAttachmentOperation("users/1", name, profileStream, "image/png"));
                    }
                    var documentDatabase = await GetDocumentDatabaseInstanceFor(store);
                    using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext mergerContext))
                    {


                        var hash = "EcDnm3HDl2zNDALRMQ4lFsCO3J2Lb1fM1oDWOk2Octo=";
                        var cmd = new AttachmentHandler.MergedPutAttachmentCommand
                        {
                            Database = documentDatabase,
                            ExpectedChangeVector = mergerContext.GetLazyString(cv1),
                            DocumentId = id,
                            Name = name,
                            Stream = profileStream,
                            Hash = hash,
                            ContentType = "image/png"
                        };
                        using (documentDatabase.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext cvContext))
                        using(cvContext.OpenReadTransaction())
                        {
                            var beforeCv = DocumentsStorage.GetDatabaseChangeVector(cvContext);
                            await Assert.ThrowsAsync<ConcurrencyException>(() => documentDatabase.TxMerger.Enqueue(cmd));
                            var afterCv = DocumentsStorage.GetDatabaseChangeVector(cvContext);
                            Assert.Equal(beforeCv, afterCv);
                        }
                        cmd = new AttachmentHandler.MergedPutAttachmentCommand
                        {
                            Database = documentDatabase,
                            ExpectedChangeVector = mergerContext.GetLazyString(cv2),
                            DocumentId = id,
                            Name = name,
                            Stream = profileStream,
                            Hash = hash,
                            ContentType = "image/png"
                        };
                        await documentDatabase.TxMerger.Enqueue(cmd);

                    }

                }
            }
        }
    }
}
