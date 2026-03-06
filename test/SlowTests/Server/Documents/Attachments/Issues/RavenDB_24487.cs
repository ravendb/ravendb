using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments.Issues
{
    public class RavenDB_24487 : RemoteAttachmentsS3Base
    {
        public RavenDB_24487(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryFact]
        public async Task ShouldThrowRemoteAttachmentIndexingExceptionWhenAccessingRemoteAttachmentAsString()
        {
            await using (var holder = CreateCloudSettings())
            {
                using var store = GetDocumentStore();
                var identifier = await SetupRemoteAttachmentsConfiguration(Settings, store);
                await CreateDocumentsWithVariousRemoteStorageUploadTimes(store, identifier);

                await AssertRemoteAttachmentIndexingException(new RemoteAttachmentIndexString(), store);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanIndexRemoteAttachmentRemoteAndRemoteAt()
        {
            await using (var holder = CreateCloudSettings())
            {
                {
                    using var store = GetDocumentStore();
                    var identifier = await SetupRemoteAttachmentsConfiguration(Settings, store);
                    DateTime baseline = await CreateDocumentsWithVariousRemoteStorageUploadTimes(store, identifier);

                    await TestRemoteAndRemoteAtIndexing(store, baseline, identifier);
                }
            }
        }

        private async Task TestRemoteAndRemoteAtIndexing(DocumentStore store, DateTime baseline, string identifier)
        {
            var index = new RemoteAttachmentIndexLoadAttachment();
            await index.ExecuteAsync(store);
            var index2 = new RemoteAttachmentIndexAttachmentsFor();
            await index2.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);
            using (var session = store.OpenSession())
            {
                var resultLoadAtt = session.Query<RemoteAttachmentIndexLoadAttachment.Result, RemoteAttachmentIndexLoadAttachment>()
                    .Where(x => (x.RemoteAt != null && x.RemoteAt > baseline.AddMinutes(2))).ToList();

                Assert.NotNull(resultLoadAtt);
                Assert.Equal(2, resultLoadAtt.Count);

                var resultLoadAttByFlag = session.Query<RemoteAttachmentIndexLoadAttachment.Result, RemoteAttachmentIndexLoadAttachment>().Where(x => x.Flags == RemoteAttachmentFlags.Remote)
                    .ProjectInto<RemoteAttachmentIndexLoadAttachment.Result>().ToList();

                Assert.Equal(RemoteAttachmentFlags.Remote, resultLoadAttByFlag[0].Flags);
                Assert.Equal(identifier, resultLoadAttByFlag[0].Identifier);
                if (resultLoadAttByFlag[0].RemoteAt != null)
                    Assert.Equal(baseline.AddMinutes(1), resultLoadAttByFlag[0].RemoteAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultLoadAttByFlag);
                Assert.Equal(1, resultLoadAttByFlag.Count);

                var resultAttFor = session.Query<RemoteAttachmentIndexAttachmentsFor.Result, RemoteAttachmentIndexAttachmentsFor>()
                    .Where(x => (x.RemoteAt != null && x.RemoteAt > baseline.AddDays(7))).ProjectInto<RemoteAttachmentIndexAttachmentsFor.Result>().ToList();

                Assert.Equal(RemoteAttachmentFlags.None, resultAttFor[0].Flags);
                Assert.Equal(identifier, resultAttFor[0].Identifier);
                if (resultAttFor[0].RemoteAt != null)
                    Assert.Equal(baseline.AddDays(365), resultAttFor[0].RemoteAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultAttFor);
                Assert.Equal(1, resultAttFor.Count);

                var resultAttForByFlag = session.Query<RemoteAttachmentIndexAttachmentsFor.Result, RemoteAttachmentIndexAttachmentsFor>().Where(x => x.Flags == RemoteAttachmentFlags.Remote)
                    .ProjectInto<RemoteAttachmentIndexAttachmentsFor.Result>().ToList();

                Assert.Equal(RemoteAttachmentFlags.Remote, resultAttForByFlag[0].Flags);
                Assert.Equal(identifier, resultAttForByFlag[0].Identifier);
                if (resultAttForByFlag[0].RemoteAt != null)
                    Assert.Equal(baseline.AddMinutes(1), resultAttForByFlag[0].RemoteAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultAttForByFlag);
                Assert.Equal(1, resultAttForByFlag.Count);
            }
        }

        private async Task<DateTime> CreateDocumentsWithVariousRemoteStorageUploadTimes(DocumentStore store, string identifier)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "John", AttName = "greeting1.txt" }, "users/1");
                await session.StoreAsync(new User { Name = "Johny", AttName = "greeting2.txt" }, "users/2");
                await session.StoreAsync(new User { Name = "JohnySilverhand", AttName = "greeting3.txt" }, "users/3");
                await session.SaveChangesAsync();
            }

            DateTime baseline = DateTime.UtcNow;
            var remoteAt1 = baseline.AddDays(7);
            var remoteAt2 = baseline.AddMinutes(1);
            var remoteAt3 = baseline.AddDays(365);

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello")))
            {
                var parameters1 = new StoreAttachmentParameters("greeting1.txt", ms) { RemoteParameters = new RemoteAttachmentParameters(identifier, remoteAt1) };
                var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                await store.Operations.SendAsync(putOp1);
                ms.Position = 0;
                var parameters2 = new StoreAttachmentParameters("greeting2.txt", ms) { RemoteParameters = new RemoteAttachmentParameters(identifier, remoteAt2) };
                var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                await store.Operations.SendAsync(putOp2);
                ms.Position = 0;
                var parameters4 = new StoreAttachmentParameters("greeting3.txt", ms) { RemoteParameters = new RemoteAttachmentParameters(identifier, remoteAt3) };
                var putOp4 = new PutAttachmentOperation("users/3", parameters4);
                await store.Operations.SendAsync(putOp4);
            }

            int count = 0;
            var remote = await WaitForValueAsync(async () =>
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                count += await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                return count;
            }, 1, interval: 1000);
            return baseline;
        }

        private static async Task<string> SetupRemoteAttachmentsConfiguration(RemoteAttachmentsS3Settings s3Settings, DocumentStore store)
        {
            var id = "conf-identifier-s3";
            var conf = new RemoteAttachmentsConfiguration
            {
                Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                {
                    {
                        id, new RemoteAttachmentsDestinationConfiguration()
                        {
                            Disabled = false,
                            S3Settings = s3Settings,
                        }
                    }
                }
            };
            await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRemoteAttachmentsOperation(conf));

            return id;
        }

        private async Task AssertRemoteAttachmentIndexingException(AbstractJavaScriptIndexCreationTask index, IDocumentStore store)
        {
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            var indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName }));
            var errorString = indexErrors[0].Errors[0].Error;
            Assert.Contains("RemoteAttachmentIndexingException: Attempted to 'GetContentAsString' on remote attachment named 'greeting2.txt' which is no longer available locally.", errorString);
        }

        private class RemoteAttachmentIndexString : AbstractJavaScriptIndexCreationTask
        {
            public RemoteAttachmentIndexString()
            {
                Maps = new HashSet<string>
                {
                    @"  
                map('Users', function (u) {  
                    return {  
                        attAsStream: loadAttachment(u, u.AttName).getContentAsString()  
                    };  
                })  
                "
                };
            }
        }

        private class RemoteAttachmentIndexAttachmentsFor : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public RemoteAttachmentFlags Flags { get; set; }
                public DateTime? RemoteAt { get; set; }
                public string Identifier { get; set; }
            }

            public RemoteAttachmentIndexAttachmentsFor()
            {
                Maps = new HashSet<string>
                {
                    @"
            map('Users', u => {
                var att = attachmentsFor(u);
                    return att.map(a => {
                        return {
                        Name: a.Name,
                        Flags: a.RemoteFlags,
                        RemoteAt: a.RemoteAt,
                        Identifier: a.RemoteIdentifier
                        };
                    });
            })
            "
            };
                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }

        private class RemoteAttachmentIndexLoadAttachment : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public RemoteAttachmentFlags Flags { get; set; }
                public DateTime? RemoteAt { get; set; }
                public string Identifier { get; set; }
            }

            public RemoteAttachmentIndexLoadAttachment()
            {

                Maps = new HashSet<string>
                {
                    @"
            map('Users', function(u) {
                var att = loadAttachment(u, u.AttName);
                return {
                    Name : att.Name,
                    Flags : att.RemoteFlags,         
                    RemoteAt: att.RemoteAt,
                    Identifier: att.RemoteIdentifier
                };
            })
            "
                };
                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public string AttName { get; set; }
        }
    }
}
