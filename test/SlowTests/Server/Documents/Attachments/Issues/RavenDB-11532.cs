using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments.Issues
{
    public class AttachmentsPatchSlowTests : RemoteAttachmentsS3Base
    {
        public AttachmentsPatchSlowTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Attachments)]
        public async Task Can_Copy_Remote_Attachment_In_Patch()
        {
            using var store = GetDocumentStore();
            await using var holder = CreateCloudSettings();

            var identifier = await PutRemoteAttachmentsConfiguration(store, Settings);

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Id = "orders/1",
                    OrderedAt = new DateTime(2024, 1, 1),
                    Company = "companies/1",
                    ShipVia = "shippers/1"
                });

                await session.StoreAsync(new Order
                {
                    Id = "orders/2",
                    OrderedAt = new DateTime(2024, 1, 1),
                    Company = "companies/2",
                    ShipVia = "shippers/2"
                });

                await session.SaveChangesAsync();
            }
            string beforeCopyCv;
            using (var commands = store.Commands())
            {
                dynamic order2Before = await commands.GetAsync("orders/2", true);
                dynamic beforeMetadata = order2Before["@metadata"];
                beforeCopyCv = beforeMetadata["@change-vector"]?.ToString();
            }
            await using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello remote attachment")))
            {
                await store.Operations.SendAsync(new PutAttachmentOperation("orders/1", "remote-file", ms, "text/plain"));
            }

            var dt = DateTime.UtcNow.AddMinutes(3);

            var markRemote = new PatchRequest
            {
                Script = "attachments(this, args.name).remote(args.identifier, args.at);",
                Values =
         {
             { "name", "remote-file" },
             { "identifier", identifier },
             { "at", dt },
         }
            };

            var patchResult = await store.Operations.SendAsync(new PatchOperation("orders/1", null, markRemote));
            Assert.Equal(PatchStatus.Patched, patchResult);

            using (var att = await store.Operations.SendAsync(
                       new GetAttachmentOperation("orders/1", "remote-file", AttachmentType.Document, null)))
            {
                Assert.Equal("remote-file", att.Details.Name);
                Assert.NotNull(att.Details.RemoteParameters);
                Assert.Equal(identifier, att.Details.RemoteParameters.Identifier);
                Assert.Equal(dt, att.Details.RemoteParameters.At);
            }

            var database = await GetDocumentDatabaseInstanceForAsync(store.Database);

            var isRemote = await WaitForValueAsync(async () =>
            {
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
                using (context.OpenReadTransaction())
                {
                    var sourceAttachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(
                        context,
                        "orders/1",
                        "remote-file",
                        AttachmentType.Document,
                        null);

                    return sourceAttachment?.RemoteParameters?.Flags == RemoteAttachmentFlags.Remote;
                }
            }, true, timeout: 15_000, interval: 1_000);

            Assert.True(isRemote);

            var cloudObjects = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
            Assert.Single(cloudObjects);

            using (database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            using (context.OpenReadTransaction())
            {
                var sourceAttachment = database.DocumentsStorage.AttachmentsStorage.GetAttachment(
                    context,
                    "orders/1",
                    "remote-file",
                    AttachmentType.Document,
                    null);

                Assert.NotNull(sourceAttachment);
                Assert.NotNull(sourceAttachment.RemoteParameters);
                Assert.Equal(RemoteAttachmentFlags.Remote, sourceAttachment.RemoteParameters.Flags);
                Assert.Equal(identifier, sourceAttachment.RemoteParameters.Identifier);
            }

            store.Operations.Send(new PatchOperation(
                "orders/2",
                null,
                new PatchRequest
                {
                    Script = @"attachments(this, 'copied-remote-file').copyFrom('orders/1', 'remote-file');"
                }));

            var cloudObjectsAfterCopy = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
            Assert.Single(cloudObjectsAfterCopy);

            using var copied = await store.Operations.SendAsync(
                new GetAttachmentOperation("orders/2", "copied-remote-file", AttachmentType.Document, null));

            Assert.NotNull(copied);
            Assert.NotNull(copied.Details);
            Assert.Equal("copied-remote-file", copied.Details.Name);
            Assert.Equal("text/plain", copied.Details.ContentType);
            Assert.NotNull(copied.Details.RemoteParameters);
            Assert.Equal(RemoteAttachmentFlags.Remote, copied.Details.RemoteParameters.Flags);
            Assert.Equal(identifier, copied.Details.RemoteParameters.Identifier);
            using (var commands = store.Commands())
            {
                dynamic order2After = await commands.GetAsync("orders/2", true);
                dynamic metadataAfter = order2After["@metadata"];
                var afterCopyCv = metadataAfter["@change-vector"]?.ToString();
                Assert.NotEqual(beforeCopyCv, afterCopyCv);

                var flags = metadataAfter["@flags"]?.ToString();
                Assert.Contains("HasAttachments", flags);
            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Attachments)]
        public void Can_Copy_Attachment_In_Patch()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new SourceDoc
                {
                    Name = "source"
                }, "source/1");

                session.Store(new TargetDoc
                {
                    Name = "target"
                }, "target/1");

                session.SaveChanges();
            }

            string beforeCopyCv;
            using (var commands = store.Commands())
            {
                dynamic targetBefore = commands.Get("target/1", true);
                Assert.NotNull(targetBefore);

                dynamic beforeMetadata = targetBefore["@metadata"];
                Assert.NotNull(beforeMetadata);
                beforeCopyCv = beforeMetadata["@change-vector"]?.ToString();
            }

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello from source attachment")))
            {
                store.Operations.Send(new PutAttachmentOperation("source/1", "thumbnail", ms, "text/plain"));
            }

            store.Operations.Send(new PatchOperation(
                "target/1",
                null,
                new PatchRequest
                {
                    Script = @"attachments(this, 'photo').copyFrom('source/1', 'thumbnail');"
                }));

            using (var result = store.Operations.Send(
                       new GetAttachmentOperation("target/1", "photo", AttachmentType.Document, null)))
            {
                Assert.NotNull(result);
                Assert.Equal("photo", result.Details.Name);
                Assert.Equal("text/plain", result.Details.ContentType);

                using var reader = new StreamReader(result.Stream);
                Assert.Equal("hello from source attachment", reader.ReadToEnd());
            }

            using (var sourceResult = store.Operations.Send(
                       new GetAttachmentOperation("source/1", "thumbnail", AttachmentType.Document, null)))
            {
                Assert.NotNull(sourceResult);
            }

            using (var commands = store.Commands())
            {
                dynamic targetAfter = commands.Get("target/1", true);
                Assert.NotNull(targetAfter);
                dynamic afterMetadata = targetAfter["@metadata"];
                Assert.NotNull(afterMetadata);

                var afterCopyCv = afterMetadata["@change-vector"]?.ToString();
                Assert.NotEqual(beforeCopyCv, afterCopyCv);

                var flags = afterMetadata["@flags"]?.ToString();
                Assert.Contains("HasAttachments", flags);

            }
        }

        [RavenFact(RavenTestCategory.Patching | RavenTestCategory.Attachments)]
        public void Can_Copy_Attachment_In_PatchByQuery()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenSession())
            {
                session.Store(new SourceDoc
                {
                    Name = "source"
                }, "source/1");

                session.Store(new TargetDoc
                {
                    Name = "target"
                }, "target/1");

                session.SaveChanges();
            }

            string beforeCopyCv;
            using (var commands = store.Commands())
            {
                dynamic targetBefore = commands.Get("target/1", true);
                Assert.NotNull(targetBefore);
                dynamic beforeMetadata = targetBefore["@metadata"];
                Assert.NotNull(beforeMetadata);
                beforeCopyCv = beforeMetadata["@change-vector"]?.ToString();
            }

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("copied by query")))
            {
                store.Operations.Send(new PutAttachmentOperation("source/1", "thumbnail", ms, "text/plain"));
            }

            var op = store.Operations.Send(new PatchByQueryOperation(new IndexQuery
            {
                Query = @"
from TargetDocs as t
update {
    attachments(t, 'photo').copyFrom('source/1', 'thumbnail');
}"
            }));
            op.WaitForCompletion();
            using (var result = store.Operations.Send(
                       new GetAttachmentOperation("target/1", "photo", AttachmentType.Document, null)))
            {
                Assert.NotNull(result);
                Assert.Equal("photo", result.Details.Name);
                Assert.Equal("text/plain", result.Details.ContentType);

                using var reader = new StreamReader(result.Stream);
                Assert.Equal("copied by query", reader.ReadToEnd());

                using (var commands = store.Commands())
                {
                    dynamic targetAfter = commands.Get("target/1", true);
                    Assert.NotNull(targetAfter);
                    dynamic afterMetadata = targetAfter["@metadata"];
                    Assert.NotNull(afterMetadata);

                    var afterCopyCv = afterMetadata["@change-vector"]?.ToString();
                    Assert.NotEqual(beforeCopyCv, afterCopyCv);

                    var flags = afterMetadata["@flags"]?.ToString();
                    Assert.Contains("HasAttachments", flags);
                }
            }
        }

        public class Order
        {
            public string Id { get; set; }
            public string Company { get; set; }
            public DateTime OrderedAt { get; set; }
            public string ShipVia { get; set; }
        }
        private sealed class SourceDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private sealed class TargetDoc
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}
