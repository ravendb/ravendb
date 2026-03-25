using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Extensions;
using Raven.Server.Exceptions.Attachments;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments.Issues
{
    public class RavenDB_24488 : RemoteAttachmentsS3Base
    {
        public RavenDB_24488(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryFact]
        public async Task ShouldThrowWhenTryingToReceiveRemoteAttachmentAsStringOrAsStream()
        {
            string remoteFolderName = "RavenDB_24488" + Guid.NewGuid();
            var s3Settings = Etl.GetS3Settings(remoteFolderName).ToRemoteAttachmentsS3Settings();
            try
            {
                using var store = GetDocumentStore();

                var conf = new RemoteAttachmentsConfiguration() 
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            "conf-identifier", new RemoteAttachmentsDestinationConfiguration()
                            {
                                Disabled = false, 
                                S3Settings = s3Settings, 
                            }
                        }
                    },
                    CheckFrequencyInSec = 1
                };

                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRemoteAttachmentsOperation(conf));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John", AttName = "greeting1.txt" }, "users/1");
                    await session.StoreAsync(new User { Name = "Johny", AttName = "greeting2.txt" }, "users/2");
                    await session.StoreAsync(new User { Name = "JohnySilverhand", AttName = "greeting3.txt" }, "users/3");
                    await session.SaveChangesAsync();
                }

                DateTime baseline = DateTime.UtcNow;
                var remoteAt1 = baseline.AddDays(7);
                var remoteAt2 = baseline.AddDays(365);
                var remoteAt3 = baseline.AddMinutes(1);

                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello")))
                {
                    var parameters1 = new StoreAttachmentParameters("greeting1.txt", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt1)
                    };
                    ;
                    var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                    await store.Operations.SendAsync(putOp1);
                    ms.Position = 0;
                    var parameters2 = new StoreAttachmentParameters("greeting2.txt", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt2)
                    };
                    ;
                    var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                    await store.Operations.SendAsync(putOp2);
                    ms.Position = 0;
                    var parameters4 = new StoreAttachmentParameters("greeting3.txt", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt3)
                    };
                    ;
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

                await AssertRemoteAttachmentIndexingExceptionAsync(new RemoteAttachmentIndexStream(), store);
                await AssertRemoteAttachmentIndexingExceptionAsync(new RemoteAttachmentIndexString(), store);
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        private async Task AssertRemoteAttachmentIndexingExceptionAsync(AbstractIndexCreationTask<User> index, IDocumentStore store)
        {
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            var indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName }));
            var errorString = indexErrors[0].Errors[0].Error;
            Assert.Contains(nameof(RemoteAttachmentIndexingException), errorString);
        }

        private class RemoteAttachmentIndexStream : AbstractIndexCreationTask<User>
        {
            public RemoteAttachmentIndexStream()
            {
                Map = users => from u in users
                    let att = LoadAttachment(u, u.AttName).GetContentAsStream()
                               select new
                               {
                                   attAsStream = att,
                               };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class RemoteAttachmentIndexString : AbstractIndexCreationTask<User>
        {
            public RemoteAttachmentIndexString()
            {
                Map = users => from u in users
                    let att = LoadAttachment(u, u.AttName).GetContentAsString()
                    select new
                    {
                        attAsStream = att,
                    };
                StoreAllFields(FieldStorage.Yes);
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
