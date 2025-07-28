using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using SlowTests.Server.Documents.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_24489 : RetiredAttachmentsS3Base
    {
        public RavenDB_24489(ITestOutputHelper output) : base(output)
        {
        }
        
        [AmazonS3RetryFact]
        public async Task Index_ShouldInclude_RetiredFlag_And_RetiredAt_ForRetiredAttachments()
        {
            string remoteFolderName = "RavenDB_24489" + Guid.NewGuid();
            var s3Settings = Etl.GetS3Settings(remoteFolderName);

            try
            {
                using var store = GetDocumentStore();

                var conf = new RetiredAttachmentsConfiguration
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "conf-identifier", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Disabled = false, 
                                S3Settings = s3Settings, 
                                Identifier = "conf-identifier",
                            }
                        }
                    },
                    RetireFrequencyInSec = 1,
                };
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRetiredAttachmentsOperation(conf));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, "users/1");
                    await session.StoreAsync(new User { Name = "Johny" }, "users/2");
                    await session.StoreAsync(new User { Name = "JohnySilverhand" }, "users/3");
                    await session.SaveChangesAsync();
                }

                DateTime baseline = DateTime.UtcNow;
                var retireAt1 = baseline.AddDays(7);
                var retireAt2 = baseline.AddDays(365);
                var retireAt3 = baseline.AddMinutes(1);

                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello")))
                {
                    var parameters1 = new StoreAttachmentParameters("greeting1.txt", ms)
                    {
                        RetireParameters = new RetireAttachmentParameters("conf-identifier", retireAt1)
                    };
                    var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                    store.Operations.Send(putOp1);
                    ms.Position = 0;

                    var parameters2 = new StoreAttachmentParameters("greeting2.txt", ms)
                    {
                        RetireParameters = new RetireAttachmentParameters("conf-identifier", retireAt2)
                    };
                    var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                    store.Operations.Send(putOp2);
                    ms.Position = 0;
                    var parameters3 = new StoreAttachmentParameters("greeting3.txt", ms)
                    {
                        RetireParameters = new RetireAttachmentParameters("conf-identifier", retireAt2)
                    };
                    var putOp3 = new PutAttachmentOperation("users/2", parameters3);
                    store.Operations.Send(putOp3);
                    ms.Position = 0;
                    var parameters4 = new StoreAttachmentParameters("greeting4.txt", ms)
                    {
                        RetireParameters = new RetireAttachmentParameters("conf-identifier", retireAt3)
                    };
                    var putOp4 = new PutAttachmentOperation("users/3", parameters4);
                    store.Operations.Send(putOp4);
                }

                int count = 0;
                var retired = await WaitForValueAsync(async () =>
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                    count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    return count;
                }, 1, interval: 1000);

                var index = new RetiredAttachmentIndex();
                await index.ExecuteAsync(store);
                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<RetiredAttachmentIndex.Result, RetiredAttachmentIndex>()
                        .Where(x => x.RetiredAt != null && x.RetiredAt > baseline.AddDays(8)).ToList();

                    Assert.NotNull(result);
                    Assert.Single(result);
                    Assert.True(result[0].Name == "Johny");
                    Assert.True(result[0].Id == "users/2");

                    var resultWithProjection = session.Query<RetiredAttachmentIndex.Result, RetiredAttachmentIndex>()
                        .Where(x => x.RetiredAt != null && x.RetiredAt > baseline).ProjectInto<RetiredAttachmentIndex.Result>().ToList();

                    Assert.NotNull(result);
                    Assert.Equal(4, resultWithProjection.Count);
                    Assert.All(resultWithProjection, x =>
                    {
                        Assert.True(x.RetiredAt.HasValue, "RetiredAt should not be null here");
                        Assert.True(x.RetiredAt.Value > baseline,
                            $"RetiredAt {x.RetiredAt} should be > baseline");
                        Assert.StartsWith("greeting", x.Name);
                    });

                    var resultByFlag = session.Query<RetiredAttachmentIndex.Result, RetiredAttachmentIndex>().Where(x => x.Retired == RetiredAttachmentFlags.Retired)
                        .ProjectInto<RetiredAttachmentIndex.Result>().ToList();

                    Assert.NotNull(resultByFlag);
                    Assert.Equal(1, resultByFlag.Count);
                }
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        [AmazonS3RetryFact]
        public async Task Index_ShouldInclude_RetiredFlag_And_RetiredAt_ForRetiredAttachments_LoadAttachments()
        {
            string remoteFolderName = "RavenDB_24489" + Guid.NewGuid();
            var s3Settings = Etl.GetS3Settings(remoteFolderName);

            try
            {
                using var store = GetDocumentStore();

                var conf = new RetiredAttachmentsConfiguration
                {
                    Destinations = new Dictionary<string, RetiredAttachmentsDestinationConfiguration>()
                    {
                        {
                            "conf-identifier", new RetiredAttachmentsDestinationConfiguration()
                            {
                                Disabled = false,
                                S3Settings = s3Settings,
                                Identifier = "conf-identifier",
                            }
                        }
                    },
                    RetireFrequencyInSec = 1,
                };
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRetiredAttachmentsOperation(conf));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John", AttName = "myAttachment.png" }, "users/1");
                    await session.StoreAsync(new User { Name = "Johny", AttName = "myPhoto.png" }, "users/2" );
                    await session.SaveChangesAsync();
                }

                DateTime baseline = DateTime.UtcNow;
                var retireAt1 = baseline.AddDays(7);
                var retireAt2 = baseline.AddMinutes(1);
                var retireAt3 = baseline.AddDays(365);
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello")))
                {
                    var parameters1 = new StoreAttachmentParameters("myAttachment.png", ms)
                    {
                        RetireParameters = new RetireAttachmentParameters("conf-identifier", retireAt1)
                    };
                    var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                    store.Operations.Send(putOp1);
                    ms.Position = 0;
                    var parameters2 = new StoreAttachmentParameters("myPhoto.png", ms)
                    {
                        RetireParameters = new RetireAttachmentParameters("conf-identifier", retireAt2)
                    };
                    var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                    store.Operations.Send(putOp2);
                    ms.Position = 0;
                    var parameters3 = new StoreAttachmentParameters("greeting3.txt", ms)
                    {
                        RetireParameters = new RetireAttachmentParameters("conf-identifier", retireAt3)
                    };
                    var putOp3 = new PutAttachmentOperation("users/2", parameters3);
                    store.Operations.Send(putOp3);
                }
                int count = 0;
                var retired = await WaitForValueAsync(async () =>
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                    count += await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    return count;
                }, 1, interval: 1000);

                var index = new RetiredAttachmentIndex();
                await index.ExecuteAsync(store);
                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<RetiredAttachmentIndex_LoadAttachments.Result, RetiredAttachmentIndex>()
                        .Where(x => x.RetiredAt != null && x.RetiredAt > baseline.AddMinutes(5)).ToList();

                    Assert.NotNull(result);
                    Assert.True(result.Count == 2);

                    var resultByFlag = session.Query<RetiredAttachmentIndex.Result, RetiredAttachmentIndex>().Where(x => x.Retired == RetiredAttachmentFlags.Retired)
                        .ProjectInto<RetiredAttachmentIndex.Result>().ToList();

                    Assert.NotNull(resultByFlag);
                    Assert.Equal(1, resultByFlag.Count);
                }
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        private class RetiredAttachmentIndex : AbstractIndexCreationTask<User, RetiredAttachmentIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public RetiredAttachmentFlags Retired { get; set; }
                public DateTime? RetiredAt { get; set; }
                public string RetiredIdentifier { get; set; }
            }

            public RetiredAttachmentIndex()
            {
                Map = users => from u in users
                    from att in AttachmentsFor(u)
                    select new Result { 
                        Name = att.Name,
                        Retired = att.RetireParameters == null ? RetiredAttachmentFlags.None : att.RetireParameters.Flags,
                        RetiredAt = att.RetireParameters == null ? null : att.RetireParameters.At,
                        RetiredIdentifier = att.RetireParameters == null ? null : att.RetireParameters.Identifier,
                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class RetiredAttachmentIndex_LoadAttachments : AbstractIndexCreationTask<User, RetiredAttachmentIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public RetiredAttachmentFlags Retired { get; set; }
                public DateTime? RetiredAt { get; set; }
            }
        
            public RetiredAttachmentIndex_LoadAttachments()
            {
                Map = users => from u in users
                    let att = LoadAttachment(u, u.AttName)
                    select new Result
                    {
                        Name = att.Name,
                        Retired = att.RetireFlags,
                        RetiredAt = att.RetireAt,
                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        public class User
        { 
            public string Id { get; set; }

            public string Name { get; set; }

            public string AttName { get; set; }
        }
    }
}
