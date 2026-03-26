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
using Raven.Client.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments.Issues
{
    public class RavenDB_24489 : RemoteAttachmentsS3Base
    {
        public RavenDB_24489(ITestOutputHelper output) : base(output)
        {
        }
        
        [AmazonS3RetryFact]
        public async Task Index_ShouldInclude_RemoteFlag_And_RemoteAt_ForRemoteAttachments()
        {
            string remoteFolderName = "RavenDB_24489" + Guid.NewGuid();
            var s3Settings = Etl.GetS3Settings(remoteFolderName).ToRemoteAttachmentsS3Settings();

            try
            {
                using var store = GetDocumentStore();

                var conf = new RemoteAttachmentsConfiguration
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
                    CheckFrequencyInSec = 1,
                };
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRemoteAttachmentsOperation(conf));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John" }, "users/1");
                    await session.StoreAsync(new User { Name = "Johny" }, "users/2");
                    await session.StoreAsync(new User { Name = "JohnySilverhand" }, "users/3");
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
                    var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                    store.Operations.Send(putOp1);
                    ms.Position = 0;

                    var parameters2 = new StoreAttachmentParameters("greeting2.txt", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt2)
                    };
                    var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                    store.Operations.Send(putOp2);
                    ms.Position = 0;
                    var parameters3 = new StoreAttachmentParameters("greeting3.txt", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt2)
                    };
                    var putOp3 = new PutAttachmentOperation("users/2", parameters3);
                    store.Operations.Send(putOp3);
                    ms.Position = 0;
                    var parameters4 = new StoreAttachmentParameters("greeting4.txt", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt3)
                    };
                    var putOp4 = new PutAttachmentOperation("users/3", parameters4);
                    store.Operations.Send(putOp4);
                }

                int count = 0;
                var remote = await WaitForValueAsync(async () =>
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                    count += await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    return count;
                }, 1, interval: 1000);

                var index = new RemoteAttachmentIndex();
                await index.ExecuteAsync(store);
                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<RemoteAttachmentIndex.Result, RemoteAttachmentIndex>()
                        .Where(x => x.RemoteAt != null && x.RemoteAt > baseline.AddDays(8)).ToList();

                    Assert.NotNull(result);
                    Assert.Single(result);
                    Assert.True(result[0].Name == "Johny");
                    Assert.True(result[0].Id == "users/2");

                    var resultWithProjection = session.Query<RemoteAttachmentIndex.Result, RemoteAttachmentIndex>()
                        .Where(x => x.RemoteAt != null && x.RemoteAt > baseline).ProjectInto<RemoteAttachmentIndex.Result>().ToList();

                    Assert.NotNull(result);
                    Assert.Equal(4, resultWithProjection.Count);
                    Assert.All(resultWithProjection, x =>
                    {
                        Assert.True(x.RemoteAt.HasValue, "RemoteAt should not be null here");
                        Assert.True(x.RemoteAt.Value > baseline,
                            $"RemoteAt {x.RemoteAt} should be > baseline");
                        Assert.StartsWith("greeting", x.Name);
                    });

                    var resultByFlag = session.Query<RemoteAttachmentIndex.Result, RemoteAttachmentIndex>().Where(x => x.Remote == RemoteAttachmentFlags.Remote)
                        .ProjectInto<RemoteAttachmentIndex.Result>().ToList();

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
        public async Task Index_ShouldInclude_RemoteFlag_And_RemoteAt_ForRemoteAttachments_LoadAttachments()
        {
            string remoteFolderName = "RavenDB_24489" + Guid.NewGuid();
            var s3Settings = Etl.GetS3Settings(remoteFolderName).ToRemoteAttachmentsS3Settings();

            try
            {
                using var store = GetDocumentStore();

                var conf = new RemoteAttachmentsConfiguration
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
                    CheckFrequencyInSec = 1,
                };
                await store.Maintenance.ForDatabase(store.Database).SendAsync(new ConfigureRemoteAttachmentsOperation(conf));

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "John", AttName = "myAttachment.png" }, "users/1");
                    await session.StoreAsync(new User { Name = "Johny", AttName = "myPhoto.png" }, "users/2" );
                    await session.SaveChangesAsync();
                }

                DateTime baseline = DateTime.UtcNow;
                var remoteAt1 = baseline.AddDays(7);
                var remoteAt2 = baseline.AddMinutes(1);
                var remoteAt3 = baseline.AddDays(365);
                using (var ms = new MemoryStream(Encoding.UTF8.GetBytes("hello")))
                {
                    var parameters1 = new StoreAttachmentParameters("myAttachment.png", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt1)
                    };
                    var putOp1 = new PutAttachmentOperation("users/1", parameters1);
                    store.Operations.Send(putOp1);
                    ms.Position = 0;
                    var parameters2 = new StoreAttachmentParameters("myPhoto.png", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt2)
                    };
                    var putOp2 = new PutAttachmentOperation("users/2", parameters2);
                    store.Operations.Send(putOp2);
                    ms.Position = 0;
                    var parameters3 = new StoreAttachmentParameters("greeting3.txt", ms)
                    {
                        RemoteParameters = new RemoteAttachmentParameters("conf-identifier", remoteAt3)
                    };
                    var putOp3 = new PutAttachmentOperation("users/2", parameters3);
                    store.Operations.Send(putOp3);
                }
                int count = 0;
                var remote = await WaitForValueAsync(async () =>
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(store);

                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(3);
                    count += await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    return count;
                }, 1, interval: 1000);

                var index = new RemoteAttachmentIndex();
                await index.ExecuteAsync(store);
                await Indexes.WaitForIndexingAsync(store);

                using (var session = store.OpenSession())
                {
                    var result = session.Query<RemoteAttachmentIndex_LoadAttachments.Result, RemoteAttachmentIndex>()
                        .Where(x => x.RemoteAt != null && x.RemoteAt > baseline.AddMinutes(5)).ToList();

                    Assert.NotNull(result);
                    Assert.True(result.Count == 2);

                    var resultByFlag = session.Query<RemoteAttachmentIndex.Result, RemoteAttachmentIndex>().Where(x => x.Remote == RemoteAttachmentFlags.Remote)
                        .ProjectInto<RemoteAttachmentIndex.Result>().ToList();

                    Assert.NotNull(resultByFlag);
                    Assert.Equal(1, resultByFlag.Count);
                }
            }
            finally
            {
                await DeleteObjects(s3Settings);
            }
        }

        private class RemoteAttachmentIndex : AbstractIndexCreationTask<User, RemoteAttachmentIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public RemoteAttachmentFlags Remote { get; set; }
                public DateTime? RemoteAt { get; set; }
                public string RemoteIdentifier { get; set; }
            }

            public RemoteAttachmentIndex()
            {
                Map = users => from u in users
                    from att in AttachmentsFor(u)
                    select new Result { 
                        Name = att.Name,
                        Remote = att.RemoteParameters == null ? RemoteAttachmentFlags.None : att.RemoteParameters.Flags,
                        RemoteAt = att.RemoteParameters == null ? null : att.RemoteParameters.At,
                        RemoteIdentifier = att.RemoteParameters == null ? null : att.RemoteParameters.Identifier,
                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        private class RemoteAttachmentIndex_LoadAttachments : AbstractIndexCreationTask<User, RemoteAttachmentIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
                public RemoteAttachmentFlags Remote { get; set; }
                public DateTime? RemoteAt { get; set; }
            }
        
            public RemoteAttachmentIndex_LoadAttachments()
            {
                Map = users => from u in users
                    let att = LoadAttachment(u, u.AttName)
                    select new Result
                    {
                        Name = att.Name,
                        Remote = att.RemoteFlags,
                        RemoteAt = att.RemoteAt,
                    };
                StoreAllFields(FieldStorage.Yes);
            }
        }

        internal class User
        { 
            public string Id { get; set; }

            public string Name { get; set; }

            public string AttName { get; set; }
        }
    }
}
