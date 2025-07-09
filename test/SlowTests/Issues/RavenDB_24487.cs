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
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Documents.Operations.Indexes;
using SlowTests.Server.Documents.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24487 : RetiredAttachmentsS3Base
    {
        public RavenDB_24487(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryFact]
        public async Task ShouldThrowRetiredAttachmentIndexingExceptionWhenAccessingRetiredAttachmentAsString()
        {
            await using(var holder = CreateCloudSettings())
            {
                using var store = GetDocumentStore();
                await SetupRetiredAttachmentsConfiguration(Settings, store);
                await CreateDocumentsWithVariousRetirementTimes(store);

                await AssertRetiredAttachmentIndexingException(new RetiredAttachmentIndexString(), store);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanIndexRetiredAttachmentRetiredAndRetiredAt()
        {
            await using (var holder = CreateCloudSettings())
            {
                {
                    using var store = GetDocumentStore();
                    await SetupRetiredAttachmentsConfiguration(Settings, store);
                    DateTime baseline = await CreateDocumentsWithVariousRetirementTimes(store);

                    await TestRetiredAndRetiredAtIndexing(store, baseline);
                }
            }
        }

        private async Task TestRetiredAndRetiredAtIndexing(DocumentStore store, DateTime baseline)
        {
            var index = new RetiredAttachmentIndexLoadAttachment();
            await index.ExecuteAsync(store);
            var index2 = new RetiredAttachmentIndexAttachmentsFor();
            await index2.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);
            using (var session = store.OpenSession())
            {
                var resultLoadAtt = session.Query<RetiredAttachmentIndexLoadAttachment.Result, RetiredAttachmentIndexLoadAttachment>()
                    .Where(x => (x.RetiredAt != null && x.RetiredAt > baseline.AddMinutes(2))).ToList();
                
                Assert.NotNull(resultLoadAtt);
                Assert.Equal(2, resultLoadAtt.Count);
                
                var resultLoadAttByFlag = session.Query<RetiredAttachmentIndexLoadAttachment.Result, RetiredAttachmentIndexLoadAttachment>().Where(x => x.Flags == AttachmentFlags.Retired)
                    .ProjectInto<RetiredAttachmentIndexLoadAttachment.Result>().ToList();
                
                Assert.Equal(AttachmentFlags.Retired, resultLoadAttByFlag[0].Flags);
                if (resultLoadAttByFlag[0].RetiredAt != null)
                    Assert.Equal(baseline.AddMinutes(1), resultLoadAttByFlag[0].RetiredAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultLoadAttByFlag);
                Assert.Equal(1, resultLoadAttByFlag.Count);

                var resultAttFor = session.Query<RetiredAttachmentIndexAttachmentsFor.Result, RetiredAttachmentIndexAttachmentsFor>()
                    .Where(x => (x.RetiredAt != null && x.RetiredAt > baseline.AddDays(7))).ProjectInto<RetiredAttachmentIndexAttachmentsFor.Result>().ToList();

                Assert.Equal(AttachmentFlags.None, resultAttFor[0].Flags);
                if (resultAttFor[0].RetiredAt != null) 
                    Assert.Equal(baseline.AddDays(365), resultAttFor[0].RetiredAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultAttFor);
                Assert.Equal(1, resultAttFor.Count);

                var resultAttForByFlag = session.Query<RetiredAttachmentIndexAttachmentsFor.Result, RetiredAttachmentIndexAttachmentsFor>().Where(x => x.Flags == AttachmentFlags.Retired)
                    .ProjectInto<RetiredAttachmentIndexAttachmentsFor.Result>().ToList();

                Assert.Equal(AttachmentFlags.Retired, resultAttForByFlag[0].Flags);
                if (resultAttForByFlag[0].RetiredAt != null) 
                    Assert.Equal(baseline.AddMinutes(1), resultAttForByFlag[0].RetiredAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultAttForByFlag);
                Assert.Equal(1, resultAttForByFlag.Count);
            }
        }

        private async Task<DateTime> CreateDocumentsWithVariousRetirementTimes(DocumentStore store)
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "John", AttName = "greeting1.txt" }, "users/1");
                await session.StoreAsync(new User { Name = "Johny", AttName = "greeting2.txt" }, "users/2");
                await session.StoreAsync(new User { Name = "JohnySilverhand", AttName = "greeting3.txt" }, "users/3");
                await session.SaveChangesAsync();
            }

            DateTime baseline = DateTime.UtcNow;
            var retireAt1 = baseline.AddDays(7);
            var retireAt2 = baseline.AddMinutes(1);
            var retireAt3 = baseline.AddDays(365);

            using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello")))
            {
                var parameters1 = new StoreAttachmentParameters("greeting1.txt", ms) { RetireAt = retireAt1 };
                var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                await store.Operations.SendAsync(putOp1);
                ms.Position = 0;
                var parameters2 = new StoreAttachmentParameters("greeting2.txt", ms) { RetireAt = retireAt2 };
                var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                await store.Operations.SendAsync(putOp2);
                ms.Position = 0;
                var parameters4 = new StoreAttachmentParameters("greeting3.txt", ms) { RetireAt = retireAt3 };
                var putOp4 = new PutAttachmentOperation("users/3", parameters4);
                await store.Operations.SendAsync(putOp4);
            }

            int count = 0;
            var retired = await WaitForValueAsync(async () =>
            {
                var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                return count;
            }, 1, interval: 1000);
            return baseline;
        }

        private static async Task SetupRetiredAttachmentsConfiguration(S3Settings s3Settings, DocumentStore store)
        {
            var conf = new RetiredAttachmentsConfiguration { Disabled = false, RetireFrequencyInSec = 1, S3Settings = s3Settings };
            await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRetiredAttachmentsOperation(conf));
        }

        private async Task AssertRetiredAttachmentIndexingException(AbstractJavaScriptIndexCreationTask index, IDocumentStore store)
        {
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            var indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName }));
            var errorString = indexErrors[0].Errors[0].Error;
            Assert.Contains("RetiredAttachmentIndexingException: Attempted to 'GetContentAsString' on retired attachment named 'greeting2.txt' which is no longer available locally.", errorString);
        }

        private class RetiredAttachmentIndexString : AbstractJavaScriptIndexCreationTask
        {
            public RetiredAttachmentIndexString()
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

        private class RetiredAttachmentIndexAttachmentsFor : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public AttachmentFlags Flags { get; set; }
                public DateTime? RetiredAt { get; set; }
            }

            public RetiredAttachmentIndexAttachmentsFor()
            {
                Maps = new HashSet<string>
                {
                    @"
            map('Users', u => {
                var att = attachmentsFor(u);
                    return att.map(a => {
                        return {
                        Name: a.Name,
                        Flags: a.Flags,
                        RetiredAt: a.RetireAt
                        };
                    });
            })
            "
            };
                Fields.Add(Constants.Documents.Indexing.Fields.AllFields, new IndexFieldOptions { Storage = FieldStorage.Yes });
            }
        }

        private class RetiredAttachmentIndexLoadAttachment : AbstractJavaScriptIndexCreationTask
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public AttachmentFlags Flags { get; set; }
                public DateTime? RetiredAt { get; set; }
            }

            public RetiredAttachmentIndexLoadAttachment()
            {

                Maps = new HashSet<string>
                {
                    @"
            map('Users', function(u) {
                var att = loadAttachment(u, u.AttName);
                return {
                    Name : att.Name,
                    Flags : att.Flags,         
                    RetiredAt: att.RetireAt      
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
