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
                var identifier = await SetupRetiredAttachmentsConfiguration(Settings, store);
                await CreateDocumentsWithVariousRetirementTimes(store, identifier);

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
                    var identifier = await SetupRetiredAttachmentsConfiguration(Settings, store);
                    DateTime baseline = await CreateDocumentsWithVariousRetirementTimes(store, identifier);

                    await TestRetiredAndRetiredAtIndexing(store, baseline, identifier);
                }
            }
        }

        private async Task TestRetiredAndRetiredAtIndexing(DocumentStore store, DateTime baseline, string identifier)
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
                
                var resultLoadAttByFlag = session.Query<RetiredAttachmentIndexLoadAttachment.Result, RetiredAttachmentIndexLoadAttachment>().Where(x => x.Flags == RetiredAttachmentFlags.Retired)
                    .ProjectInto<RetiredAttachmentIndexLoadAttachment.Result>().ToList();
                
                Assert.Equal(RetiredAttachmentFlags.Retired, resultLoadAttByFlag[0].Flags);
                Assert.Equal(identifier, resultLoadAttByFlag[0].Identifier);
                if (resultLoadAttByFlag[0].RetiredAt != null)
                    Assert.Equal(baseline.AddMinutes(1), resultLoadAttByFlag[0].RetiredAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultLoadAttByFlag);
                Assert.Equal(1, resultLoadAttByFlag.Count);

                var resultAttFor = session.Query<RetiredAttachmentIndexAttachmentsFor.Result, RetiredAttachmentIndexAttachmentsFor>()
                    .Where(x => (x.RetiredAt != null && x.RetiredAt > baseline.AddDays(7))).ProjectInto<RetiredAttachmentIndexAttachmentsFor.Result>().ToList();

                Assert.Equal(RetiredAttachmentFlags.None, resultAttFor[0].Flags);
                Assert.Equal(identifier, resultAttFor[0].Identifier);
                if (resultAttFor[0].RetiredAt != null)
                    Assert.Equal(baseline.AddDays(365), resultAttFor[0].RetiredAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultAttFor);
                Assert.Equal(1, resultAttFor.Count);

                var resultAttForByFlag = session.Query<RetiredAttachmentIndexAttachmentsFor.Result, RetiredAttachmentIndexAttachmentsFor>().Where(x => x.Flags == RetiredAttachmentFlags.Retired)
                    .ProjectInto<RetiredAttachmentIndexAttachmentsFor.Result>().ToList();

                Assert.Equal(RetiredAttachmentFlags.Retired, resultAttForByFlag[0].Flags);
                Assert.Equal(identifier, resultAttForByFlag[0].Identifier);
                if (resultAttForByFlag[0].RetiredAt != null) 
                    Assert.Equal(baseline.AddMinutes(1), resultAttForByFlag[0].RetiredAt.Value, TimeSpan.FromMilliseconds(1));
                Assert.NotNull(resultAttForByFlag);
                Assert.Equal(1, resultAttForByFlag.Count);
            }
        }

        private async Task<DateTime> CreateDocumentsWithVariousRetirementTimes(DocumentStore store, string identifier)
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
                var parameters1 = new StoreAttachmentParameters("greeting1.txt", ms) { RetireParameters = new RetireAttachmentParameters(identifier, retireAt1)};
                var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                await store.Operations.SendAsync(putOp1);
                ms.Position = 0;
                var parameters2 = new StoreAttachmentParameters("greeting2.txt", ms) { RetireParameters = new RetireAttachmentParameters(identifier, retireAt2) };
                var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                await store.Operations.SendAsync(putOp2);
                ms.Position = 0;
                var parameters4 = new StoreAttachmentParameters("greeting3.txt", ms) { RetireParameters = new RetireAttachmentParameters(identifier, retireAt3) };
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

        private static async Task<string> SetupRetiredAttachmentsConfiguration(S3Settings s3Settings, DocumentStore store)
        {
            var id = "conf-identifier-s3";
            var conf = new RetiredAttachmentsConfiguration
            {
                Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                {
                    {
                        id, new RetiredAttachmentsDestinationConfiguration()
                        {
                            Disabled = false, 
                            S3Settings = s3Settings,
                            Identifier = id
                        }
                    }
                },
                RetireFrequencyInSec = 1
            };
            await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRetiredAttachmentsOperation(conf));

            return id;
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
                public RetiredAttachmentFlags Flags { get; set; }
                public DateTime? RetiredAt { get; set; }
                public string Identifier { get; set; }
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
                        Flags: a.RetireFlags,
                        RetiredAt: a.RetireAt,
                        Identifier: a.RetireIdentifier
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
                public RetiredAttachmentFlags Flags { get; set; }
                public DateTime? RetiredAt { get; set; }
                public string Identifier { get; set; }
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
                    Flags : att.RetireFlags,         
                    RetiredAt: att.RetireAt,
                    Identifier: att.RetireIdentifier
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
