using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Exceptions;
using SlowTests.Server.Documents.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
namespace SlowTests.Issues
{
    public class RavenDB_24488 : RetiredAttachmentsS3Base
    {
        public RavenDB_24488(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryFact]
        public async Task ShouldThrowWhenTryingToReceiveRetiredAttachmentAsStringOrAsStream()
        {
            string remoteFolderName = "RavenDB_24488";
            var s3Settings = Etl.GetS3Settings(remoteFolderName);
            try
            {
                using var store = GetDocumentStore();

                var conf = new RetiredAttachmentsConfiguration { Disabled = false, RetireFrequencyInSec = 1, S3Settings = s3Settings };
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRetiredAttachmentsOperation(conf));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John", AttName = "greeting1.txt" }, "users/1");
                    await session.StoreAsync(new User { Name = "Johny", AttName = "greeting2.txt" }, "users/2");
                    await session.StoreAsync(new User { Name = "JohnySilverhand", AttName = "greeting3.txt" }, "users/3");
                    await session.SaveChangesAsync();
                }

                DateTime baseline = DateTime.UtcNow;
                var retireAt1 = baseline.AddDays(7);
                var retireAt2 = baseline.AddDays(365);
                var retireAt3 = baseline.AddMinutes(1);

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

                await AssertRetiredAttachmentIndexingExceptionAsync(new RetiredAttachmentIndexStream(), store);
                await AssertRetiredAttachmentIndexingExceptionAsync(new RetiredAttachmentIndexString(), store);
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        private async Task AssertRetiredAttachmentIndexingExceptionAsync(AbstractIndexCreationTask<User> index, IDocumentStore store)
        {
            await index.ExecuteAsync(store);
            await Indexes.WaitForIndexingAsync(store);

            var indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { index.IndexName }));
            var errorString = indexErrors[0].Errors[0].Error;
            Assert.Contains(nameof(RetiredAttachmentIndexingException), errorString);
        }

        private class RetiredAttachmentIndexStream : AbstractIndexCreationTask<User>
        {
            public RetiredAttachmentIndexStream()
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

        private class RetiredAttachmentIndexString : AbstractIndexCreationTask<User>
        {
            public RetiredAttachmentIndexString()
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
