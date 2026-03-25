using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Remote;
using Raven.Client.Extensions;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.Attachments
{
    public class DocumentSessionRemoteAttachmentsAsyncTests : RemoteAttachmentsS3Base
    {
        //TODO: egor add cluster wide session test
        public DocumentSessionRemoteAttachmentsAsyncTests(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryFact]
        public async Task CanUploadRemoteAttachmentsToMultipleDestinations()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier2 = "Conf-identifier-s3-2";
                var settings = Etl.GetS3Settings(nameof(RemoteAttachments), $"{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();
                ModifyRemoteAttachmentsConfig = config =>
                {
                    config.Destinations.Add(identifier2, new RemoteAttachmentsDestinationConfiguration
                    {
                        S3Settings = settings,
                        Disabled = false,
                    });
                };

                var identifier1 = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);

                var baseline = DateTime.UtcNow;
                var id1 = "Orders/1";
                var id2 = "Orders/2";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id1, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                var at1 = baseline.AddMinutes(3);
                var at2 = baseline.AddMinutes(3);
                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id1, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier1, at1),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id2, OrderedAt = new DateTime(2025, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([3, 2, 2]);
                    session.Advanced.Attachments.Store(id2, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier2, at2),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var exists1 = session.Advanced.Attachments.Get(id1, "test.png");
                    Assert.NotNull(exists1);
                    Assert.Equal("test.png", exists1.Details.Name);
                    Assert.Equal("image/png", exists1.Details.ContentType);

                    Assert.NotNull(exists1.Details.RemoteParameters);
                    Assert.Equal(RemoteAttachmentFlags.None, exists1.Details.RemoteParameters.Flags);
                    Assert.Equal(identifier1, exists1.Details.RemoteParameters.Identifier);
                    Assert.Equal(at1, exists1.Details.RemoteParameters.At.ToUniversalTime());

                    var exists2 = session.Advanced.Attachments.Get(id2, "test.png");
                    Assert.NotNull(exists2);
                    Assert.Equal("test.png", exists2.Details.Name);
                    Assert.Equal("image/png", exists2.Details.ContentType);

                    Assert.NotNull(exists2.Details.RemoteParameters);
                    Assert.Equal(RemoteAttachmentFlags.None, exists2.Details.RemoteParameters.Flags);
                    Assert.Equal(identifier2, exists2.Details.RemoteParameters.Identifier);
                    Assert.Equal(at2, exists2.Details.RemoteParameters.At.ToUniversalTime());
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                var filesOnCloud1 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                var filesOnCloud2 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                Assert.Equal(1, filesOnCloud1.Count);
                Assert.Equal(1, filesOnCloud2.Count);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanCheckIfRemoteAttachmentExists()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var remoteExists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(remoteExists);
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(null)]
        [InlineData(15)]
        public async Task CanChangeRemoteAtOfAttachment(int? minutesToAdd)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                if (minutesToAdd.HasValue == false)
                {
                    using (var session = store.OpenSession())
                    {
                        using var profileStream = new MemoryStream([1, 2, 3]);
                        session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                        {
                            RemoteParameters = null,
                            ContentType = "image/png"
                        });
                        session.SaveChanges();
                    }

                    // try to remote  - nothing should happen
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                    using (var session = store.OpenSession())
                    {
                        var attachment = session.Advanced.Attachments.Get(id, "test.png");

                        Assert.NotNull(attachment);
                        Assert.Equal("test.png", attachment.Details.Name);
                        Assert.Null(attachment.Details.RemoteParameters);
                        Assert.Equal("image/png", attachment.Details.ContentType);
                    }
                }
                else
                {
                    var remoteAtDate = DateTime.UtcNow.AddMinutes(minutesToAdd.Value);
                    using (var session = store.OpenSession())
                    {
                        using var profileStream = new MemoryStream([1, 2, 3]);
                        session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                        {
                            RemoteParameters = new RemoteAttachmentParameters(identifier, remoteAtDate),
                            ContentType = "image/png"
                        });
                        session.SaveChanges();
                    }

                    // try to remote  - nothing should happen
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                    using (var session = store.OpenSession())
                    {
                        var attachment = session.Advanced.Attachments.Get(id, "test.png");

                        Assert.NotNull(attachment);
                        Assert.Equal("test.png", attachment.Details.Name);
                        Assert.Equal(RemoteAttachmentFlags.None, attachment.Details.RemoteParameters.Flags);
                        Assert.Equal(remoteAtDate, attachment.Details.RemoteParameters.At);
                        Assert.Equal("image/png", attachment.Details.ContentType);
                    }
                }

            }
        }

        [AmazonS3RetryFact]
        public async Task CanChangeIdentifierOfAttachment()
        {
            string newIdentifier = "Conf-identifier-s3-2";
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                RemoteAttachmentsS3Settings settings = null;
                // Create second configuration with different identifier
                settings = Etl.GetS3Settings(nameof(RemoteAttachments), $"{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();
                ModifyRemoteAttachmentsConfig = config =>
                {
                    config.Destinations.Add(newIdentifier, new RemoteAttachmentsDestinationConfiguration
                    {
                        S3Settings = settings,
                        Disabled = false,
                    });
                };

                var identifier1 = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);

                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                var remoteAtDate = DateTime.UtcNow.AddMinutes(3);
                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier1, remoteAtDate),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(newIdentifier, remoteAtDate),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                // try to remote - nothing should happen yet (time not reached)
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 0);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.None, attachment.Details.RemoteParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RemoteParameters.Identifier);
                    Assert.Equal(remoteAtDate, attachment.Details.RemoteParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);
                }

                // now remote - should work with new identifier
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 1);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RemoteParameters.Identifier);
                    Assert.Equal(remoteAtDate, attachment.Details.RemoteParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRemoteAttachmentByDocumentIdAndName()
        {
            using (var store = GetDocumentStore())
            await using (var holder = CreateCloudSettings())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/2";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanChangeIdentifierAndRemoteAtOfAttachment()
        {
            string newIdentifier = "Conf-identifier-s3-2";
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                RemoteAttachmentsS3Settings settings = null;
                // Create second configuration with different identifier
                settings = Etl.GetS3Settings(nameof(RemoteAttachments), $"{Guid.NewGuid()}").ToRemoteAttachmentsS3Settings();
                ModifyRemoteAttachmentsConfig = config =>
                {
                    config.Destinations.Add(newIdentifier, new RemoteAttachmentsDestinationConfiguration
                    {
                        S3Settings = settings,
                        Disabled = false,
                    });
                };

                var identifier1 = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);

                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                var originalRemoteAtDate = DateTime.UtcNow.AddMinutes(3);
                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(identifier1, originalRemoteAtDate),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                // Change both identifier and remote time
                var newRemoteAtDate = DateTime.UtcNow.AddMinutes(15);
                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RemoteParameters = new RemoteAttachmentParameters(newIdentifier, newRemoteAtDate),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                // try to remote - nothing should happen yet (time not reached)
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 0);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.None, attachment.Details.RemoteParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RemoteParameters.Identifier);
                    Assert.Equal(newRemoteAtDate, attachment.Details.RemoteParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);

                    // Verify it's not the original parameters
                    Assert.NotEqual(identifier1, attachment.Details.RemoteParameters.Identifier);
                    Assert.NotEqual(originalRemoteAtDate, attachment.Details.RemoteParameters.At);
                }

                // try to remote - nothing should happen yet (time not reached)
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(settings, 0);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.None, attachment.Details.RemoteParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RemoteParameters.Identifier);
                    Assert.Equal(newRemoteAtDate, attachment.Details.RemoteParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);

                    // Verify it's not the original parameters
                    Assert.NotEqual(identifier1, attachment.Details.RemoteParameters.Identifier);
                    Assert.NotEqual(originalRemoteAtDate, attachment.Details.RemoteParameters.At);
                }

                // now remote - should work with new identifier and new time
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(20);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 1);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RemoteParameters.Identifier);
                    Assert.Equal(newRemoteAtDate, attachment.Details.RemoteParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRemoteAttachmentByEntityAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/3";
                using (var session = store.OpenSession())
                {
                    var order = new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/3", Company = $"Companies/3" };
                    session.Store(order);
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    var attachment = session.Advanced.Attachments.Get(order, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetEnumeratorOfRemoteAttachments()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var attachments = session.Advanced.Attachments.Get(new List<AttachmentRequest> { new AttachmentRequest(id, "test.png") });
                    Assert.NotNull(attachments);
                    Assert.True(attachments.MoveNext());
                    var attachment = attachments.Current;
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDeleteAttachmentWithSameHashAsRemoteAttachment(bool flag)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";

                using (var session = store.OpenAsyncSession())
                {
                    //save remote
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                if (flag)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                }

                using (var session = store.OpenAsyncSession())
                {
                    //save attachment with the same hash
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test2.png", profileStream) { ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                if (flag == false)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete(id, "test.png");
                    session.SaveChanges();

                    Assert.False(session.Advanced.Attachments.Exists(id, "test.png"));
                    Assert.True(session.Advanced.Attachments.Exists(id, "test2.png"));
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDeleteAttachmentWithSameHashAsRemoteAttachment2(bool flag)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";

                using (var session = store.OpenAsyncSession())
                {
                    //save remote
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                if (flag)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                }

                using (var session = store.OpenAsyncSession())
                {
                    //save attachment with the same hash
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test2.png", profileStream) { ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                if (flag == false)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                }

                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete(id, "test2.png");
                    session.SaveChanges();

                    Assert.True(session.Advanced.Attachments.Exists(id, "test.png"));
                    Assert.False(session.Advanced.Attachments.Exists(id, "test2.png"));
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRemoteAttachmentByDocumentIdAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete(id, "test.png");
                    session.SaveChanges();

                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.False(exists);
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRemoteAttachmentByEntityAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/6";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/6", Company = $"Companies/6" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    session.Advanced.Attachments.Delete(order, "test.png");
                    session.SaveChanges();

                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.False(exists);
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanCheckIfRemoteAttachmentExistsAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/1";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Store(id,
                        new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var exists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.True(exists);
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var remoteExists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.True(remoteExists);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRemoteAttachmentByDocumentIdAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/2";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var attachment = await session.Advanced.Attachments.GetAsync(id, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.NotNull(attachment.Details.RemoteParameters);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRemoteAttachmentByEntityAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    var order = new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/3", Company = $"Companies/3" };
                    await session.StoreAsync(order);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }


                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    var attachment = await session.Advanced.Attachments.GetAsync(order, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetEnumeratorOfRemoteAttachmentsAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var attachments = await session.Advanced.Attachments.GetAsync(new List<AttachmentRequest> { new AttachmentRequest(id, "test.png") });
                    Assert.NotNull(attachments);
                    Assert.True(attachments.MoveNext());
                    var attachment = attachments.Current;
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRemoteAttachmentByEntityAndNameFromLocalAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                var id2 = "Order/44";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id2, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id2, new StoreAttachmentParameters("testtesttest.png", profileStream));
                    await session.SaveChangesAsync();
                }

                // I put dummy config for the old identifier so the attachment will be read from local storage
                var config = new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            identifier, new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "EGOR"
                                },
                                Disabled = false,
                            }
                        }
                    },
                    CheckFrequencyInSec = 1000
                };

                ModifyRemoteAttachmentsConfig?.Invoke(config);
                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(config));

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    var attachment = await session.Advanced.Attachments.GetAsync(order, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetEnumeratorOfRemoteAttachmentsFromLocalAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                var id2 = "Order/44";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id2, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    await session.SaveChangesAsync();
                }
                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id2, new StoreAttachmentParameters("testtesttest.png", profileStream));
                    await session.SaveChangesAsync();
                }

                // I put dummy config for the old identifier so the attachment will be read from local storage
                var config = new RemoteAttachmentsConfiguration()
                {
                    Destinations = new Dictionary<string, RemoteAttachmentsDestinationConfiguration>()
                    {
                        {
                            identifier, new RemoteAttachmentsDestinationConfiguration()
                            {
                                S3Settings = new RemoteAttachmentsS3Settings()
                                {
                                    BucketName = "EGOR"
                                },
                                Disabled = false,
                            }
                        }
                    },
                    CheckFrequencyInSec = 1000
                };

                ModifyRemoteAttachmentsConfig?.Invoke(config);
                await store.Maintenance.SendAsync(new ConfigureRemoteAttachmentsOperation(config));


                using (var session = store.OpenAsyncSession())
                {
                    var attachments = await session.Advanced.Attachments.GetAsync(new List<AttachmentRequest> { new AttachmentRequest(id, "test.png") });
                    Assert.NotNull(attachments);
                    Assert.True(attachments.MoveNext());
                    var attachment = attachments.Current;
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRemoteAttachmentByDocumentIdAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Delete(id, "test.png");
                    await session.SaveChangesAsync();

                    var exists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.False(exists);
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }


        [AmazonS3RetryFact]
        public async Task CanDeleteRemoteAttachmentByEntityAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/6";
                using (var session = store.OpenAsyncSession())
                {
                    var order = new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/6", Company = $"Companies/6" };
                    await session.StoreAsync(order);
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    session.Advanced.Attachments.Delete(order, "test.png");
                    await session.SaveChangesAsync();

                    var exists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.False(exists);
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(new byte[] { 1, 2, 3 })]
        [InlineData(new byte[] { 3, 2, 1 })]
        public async Task CanOverwriteRemoteAttachment(byte[] buffer)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                var buf = new byte[] { 1, 2, 3 };
                using var profileStream = new MemoryStream(buf);
                await PutAttachmentForTests(store, id, "test.png", profileStream, identifier);
                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                var blobs1 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                GetStorageAttachmentsMetadataFromAllAttachments(database);
                var a = Attachments.First();
                var remoteKey = $"{Settings.RemoteFolderName}/{a.Hash}";

                Assert.Equal(blobs1.First().FullPath, remoteKey);

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream2 = new MemoryStream(buffer);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream2) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                // we should still have the remote attachment stream in the cloud
                S3RemoteAttachmentsSlowTests.GetToRemoteAttachmentsCount(database, 1);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(1);
                // nothing should happen
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                S3RemoteAttachmentsSlowTests.GetToRemoteAttachmentsCount(database, 1);

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);

                    var attachment = session.Advanced.Attachments.Get(id, "test.png");
                    using var ms = new MemoryStream();
                    await attachment.Stream.CopyToAsync(ms);
                    Assert.Equal(buffer, ms.ToArray());
                }

                // put remote attachment is processed
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                List<FileInfoDetails> blobs2;
                if (buf.SequenceEqual(buffer))
                {
                    blobs2 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                }
                else
                {
                    blobs2 = await GetBlobsFromCloudAndAssertForCount(Settings, 2, 15_000);
                }

                S3RemoteAttachmentsSlowTests.GetToRemoteAttachmentsCount(database, 0);
                GetStorageAttachmentsMetadataFromAllAttachments(database);

                a = Attachments.First();
                Assert.Equal(blobs2.First().FullPath, $"{Settings.RemoteFolderName}/{a.Hash}");
                Assert.Equal(1, Attachments.Count);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRemoteAttachmentByDocumentIdAndNameAndRead()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRemoteAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    session.SaveChanges();
                }

                var rnd = new Random();
                var b = new byte[3];
                rnd.NextBytes(b);

                using var profileStream = new MemoryStream(b);
                using (var session = store.OpenAsyncSession())
                {
                    session.Advanced.Attachments.Store(id,
                        new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);
                using (var session = store.OpenSession())
                {
                    session.Advanced.Attachments.Delete(id, "test.png");
                    session.SaveChanges();

                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.False(exists);
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);

                // add attachment with same name but different
                rnd = new Random();
                b = new byte[3];
                rnd.NextBytes(b);

                using (var session = store.OpenAsyncSession())
                {
                    using var newProfileStream = new MemoryStream(b);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", newProfileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                await database.RemoteAttachmentsSender.ProcessRemoteAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var remoteExists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(remoteExists);

                    var attachment = session.Advanced.Attachments.Get(id, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RemoteAttachmentFlags.Remote, attachment.Details.RemoteParameters.Flags);

                    // compare streams
                    using var ms = new MemoryStream();
                    await attachment.Stream.CopyToAsync(ms);
                    Assert.Equal(b, ms.ToArray());

                    await GetBlobsFromCloudAndAssertForCount(Settings, 2);
                }
            }
        }

        private async Task PutAttachmentForTests(DocumentStore store, string id, string name, MemoryStream profileStream, string identifier)
        {
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Store(id,
                    new StoreAttachmentParameters("test.png", profileStream) { RemoteParameters = new RemoteAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                await session.SaveChangesAsync();
            }

            profileStream.Position = 0;

            Attachments.RemoveAll(x => x.DocumentId.ToLowerInvariant() == id && x.Name == name);

            Attachments.Add(new RemoteAttachment()
            {
                Name = name,
                DocumentId = id,
                Stream = profileStream,
                ContentType = "image/png"
            });
        }
    }
}
