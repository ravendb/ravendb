using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Backups;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class DocumentSessionRetiredAttachmentsAsyncTests : RetiredAttachmentsS3Base
    {
        //TODO: egor add cluster wide session test
        public DocumentSessionRetiredAttachmentsAsyncTests(ITestOutputHelper output) : base(output)
        {
        }

        [AmazonS3RetryFact]
        public async Task CanUploadRetiredAttachmentsToMultipleDestinations()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier2 = "Conf-identifier-s3-2";
                var settings = Etl.GetS3Settings(nameof(RetiredAttachments), $"{Guid.NewGuid()}");
                ModifyRetiredAttachmentsConfig = config =>
                {
                    config.Destinations.Add(identifier2, new RetiredAttachmentsDestinationConfiguration
                    {
                        S3Settings = settings,
                        Disabled = false,
                        Identifier = identifier2
                    });
                };

                var identifier1 = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);

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
                        RetireParameters = new RetireAttachmentParameters(identifier1, at1),
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
                        RetireParameters = new RetireAttachmentParameters(identifier2, at2),
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

                    Assert.NotNull(exists1.Details.RetireParameters);
                    Assert.Equal(RetiredAttachmentFlags.None, exists1.Details.RetireParameters.Flags);
                    Assert.Equal(identifier1, exists1.Details.RetireParameters.Identifier);
                    Assert.Equal(at1, exists1.Details.RetireParameters.At.ToUniversalTime());

                    var exists2 = session.Advanced.Attachments.Get(id2, "test.png");
                    Assert.NotNull(exists2);
                    Assert.Equal("test.png", exists2.Details.Name);
                    Assert.Equal("image/png", exists2.Details.ContentType);

                    Assert.NotNull(exists2.Details.RetireParameters);
                    Assert.Equal(RetiredAttachmentFlags.None, exists2.Details.RetireParameters.Flags);
                    Assert.Equal(identifier2, exists2.Details.RetireParameters.Identifier);
                    Assert.Equal(at2, exists2.Details.RetireParameters.At.ToUniversalTime());
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                var filesOnCloud1 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                var filesOnCloud2 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                Assert.Equal(1, filesOnCloud1.Count);
                Assert.Equal(1, filesOnCloud2.Count);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanCheckIfRetiredAttachmentExists()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                        RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
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
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var retiredExists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(retiredExists);
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(null)]
        [InlineData(15)]
        public async Task CanChangeRetireAtOfAttachment(int? minutesToAdd)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                        RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)),
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
                            RetireParameters = null,
                            ContentType = "image/png"
                        });
                        session.SaveChanges();
                    }

                    // try to retire  - nothing should happen
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                    using (var session = store.OpenSession())
                    {
                        var attachment = session.Advanced.Attachments.Get(id, "test.png");

                        Assert.NotNull(attachment);
                        Assert.Equal("test.png", attachment.Details.Name);
                        Assert.Null(attachment.Details.RetireParameters);
                        Assert.Equal("image/png", attachment.Details.ContentType);
                    }
                }
                else
                {
                    var retireAtDate = DateTime.UtcNow.AddMinutes(minutesToAdd.Value);
                    using (var session = store.OpenSession())
                    {
                        using var profileStream = new MemoryStream([1, 2, 3]);
                        session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                        {
                            RetireParameters = new RetireAttachmentParameters(identifier, retireAtDate),
                            ContentType = "image/png"
                        });
                        session.SaveChanges();
                    }

                    // try to retire  - nothing should happen
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                    using (var session = store.OpenSession())
                    {
                        var attachment = session.Advanced.Attachments.Get(id, "test.png");

                        Assert.NotNull(attachment);
                        Assert.Equal("test.png", attachment.Details.Name);
                        Assert.Equal(RetiredAttachmentFlags.None, attachment.Details.RetireParameters.Flags);
                        Assert.Equal(retireAtDate, attachment.Details.RetireParameters.At);
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
                S3Settings settings = null;
                // Create second configuration with different identifier
                settings = Etl.GetS3Settings(nameof(RetiredAttachments), $"{Guid.NewGuid()}");
                ModifyRetiredAttachmentsConfig = config =>
                {
                    config.Destinations.Add(newIdentifier, new RetiredAttachmentsDestinationConfiguration
                    {
                        S3Settings = settings,
                        Disabled = false,
                        Identifier = newIdentifier
                    });
                };

                var identifier1 = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);

                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                var retireAtDate = DateTime.UtcNow.AddMinutes(3);
                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RetireParameters = new RetireAttachmentParameters(identifier1, retireAtDate),
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
                        RetireParameters = new RetireAttachmentParameters(newIdentifier, retireAtDate),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                // try to retire - nothing should happen yet (time not reached)
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 0);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.None, attachment.Details.RetireParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RetireParameters.Identifier);
                    Assert.Equal(retireAtDate, attachment.Details.RetireParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);
                }

                // now retire - should work with new identifier
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 1);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RetireParameters.Identifier);
                    Assert.Equal(retireAtDate, attachment.Details.RetireParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRetiredAttachmentByDocumentIdAndName()
        {
            using (var store = GetDocumentStore())
            await using (var holder = CreateCloudSettings())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/2";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanChangeIdentifierAndRetireAtOfAttachment()
        {
            string newIdentifier = "Conf-identifier-s3-2";
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                S3Settings settings = null;
                // Create second configuration with different identifier
                settings = Etl.GetS3Settings(nameof(RetiredAttachments), $"{Guid.NewGuid()}");
                ModifyRetiredAttachmentsConfig = config =>
                {
                    config.Destinations.Add(newIdentifier, new RetiredAttachmentsDestinationConfiguration
                    {
                        S3Settings = settings,
                        Disabled = false,
                        Identifier = newIdentifier
                    });
                };

                var identifier1 = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);

                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                var originalRetireAtDate = DateTime.UtcNow.AddMinutes(3);
                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RetireParameters = new RetireAttachmentParameters(identifier1, originalRetireAtDate),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                // Change both identifier and retire time
                var newRetireAtDate = DateTime.UtcNow.AddMinutes(15);
                using (var session = store.OpenSession())
                {
                    using var profileStream = new MemoryStream([1, 2, 3]);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream)
                    {
                        RetireParameters = new RetireAttachmentParameters(newIdentifier, newRetireAtDate),
                        ContentType = "image/png"
                    });
                    session.SaveChanges();
                }

                // try to retire - nothing should happen yet (time not reached)
                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 0);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.None, attachment.Details.RetireParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RetireParameters.Identifier);
                    Assert.Equal(newRetireAtDate, attachment.Details.RetireParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);

                    // Verify it's not the original parameters
                    Assert.NotEqual(identifier1, attachment.Details.RetireParameters.Identifier);
                    Assert.NotEqual(originalRetireAtDate, attachment.Details.RetireParameters.At);
                }

                // try to retire - nothing should happen yet (time not reached)
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(settings, 0);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.None, attachment.Details.RetireParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RetireParameters.Identifier);
                    Assert.Equal(newRetireAtDate, attachment.Details.RetireParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);

                    // Verify it's not the original parameters
                    Assert.NotEqual(identifier1, attachment.Details.RetireParameters.Identifier);
                    Assert.NotEqual(originalRetireAtDate, attachment.Details.RetireParameters.At);
                }

                // now retire - should work with new identifier and new time
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(20);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(settings, 1);
                await GetBlobsFromCloudAndAssertForCount(Settings, 0);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.Attachments.Get(id, "test.png");

                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                    Assert.Equal(newIdentifier, attachment.Details.RetireParameters.Identifier);
                    Assert.Equal(newRetireAtDate, attachment.Details.RetireParameters.At);
                    Assert.Equal("image/png", attachment.Details.ContentType);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRetiredAttachmentByEntityAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    var attachment = session.Advanced.Attachments.Get(order, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetEnumeratorOfRetiredAttachments()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var attachments = session.Advanced.Attachments.Get(new List<AttachmentRequest> { new AttachmentRequest(id, "test.png") });
                    Assert.NotNull(attachments);
                    Assert.True(attachments.MoveNext());
                    var attachment = attachments.Current;
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                }
            }
        }

        [AmazonS3RetryTheory]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDeleteAttachmentWithSameHashAsRetiredAttachment(bool flag)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";

                using (var session = store.OpenAsyncSession())
                {
                    //save retired
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                if (flag)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
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
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
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
        public async Task CanDeleteAttachmentWithSameHashAsRetiredAttachment2(bool flag)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";

                using (var session = store.OpenAsyncSession())
                {
                    //save retired
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                if (flag)
                {
                    var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                    database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
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
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
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
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
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
        public async Task CanDeleteRetiredAttachmentByEntityAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/6";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/6", Company = $"Companies/6" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

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
        public async Task CanCheckIfRetiredAttachmentExistsAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                        new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    var exists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.True(exists);
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var retiredExists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.True(retiredExists);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRetiredAttachmentByDocumentIdAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/2";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var attachment = await session.Advanced.Attachments.GetAsync(id, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.NotNull(attachment.Details.RetireParameters);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetRetiredAttachmentByEntityAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }


                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    var attachment = await session.Advanced.Attachments.GetAsync(order, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanGetEnumeratorOfRetiredAttachmentsAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var attachments = await session.Advanced.Attachments.GetAsync(new List<AttachmentRequest> { new AttachmentRequest(id, "test.png") });
                    Assert.NotNull(attachments);
                    Assert.True(attachments.MoveNext());
                    var attachment = attachments.Current;
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);
                }
            }
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

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
        public async Task CanDeleteRetiredAttachmentByEntityAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

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
        public async Task CanOverwriteRetireAttachment(byte[] buffer)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                var blobs1 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);

                GetStorageAttachmentsMetadataFromAllAttachments(database);
                var a = Attachments.First();
                var retireKey = $"{Settings.RemoteFolderName}/{a.Hash}";

                Assert.Equal(blobs1.First().FullPath, retireKey);

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream2 = new MemoryStream(buffer);
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream2) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                // we should still have the retired attachment stream in the cloud
                S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(database, 1);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(1);
                // nothing should happen
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(database, 1);

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);

                    var attachment = session.Advanced.Attachments.Get(id, "test.png");
                    using var ms = new MemoryStream();
                    await attachment.Stream.CopyToAsync(ms);
                    Assert.Equal(buffer, ms.ToArray());
                }

                // put retire attachment is processed
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(5);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                List<FileInfoDetails> blobs2;
                if (buf.SequenceEqual(buffer))
                {
                    blobs2 = await GetBlobsFromCloudAndAssertForCount(Settings, 1, 15_000);
                }
                else
                {
                    blobs2 = await GetBlobsFromCloudAndAssertForCount(Settings, 2, 15_000);
                }

                S3RetiredAttachmentsSlowTests.GetToRetireAttachmentsCount(database, 0);
                GetStorageAttachmentsMetadataFromAllAttachments(database);

                a = Attachments.First();
                Assert.Equal(blobs2.First().FullPath, $"{Settings.RemoteFolderName}/{a.Hash}");
                Assert.Equal(1, Attachments.Count);
            }
        }

        [AmazonS3RetryFact]
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndNameAndRead()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                var identifier = await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                        new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", newProfileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenSession())
                {
                    var exists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(exists);
                }

                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var retiredExists = session.Advanced.Attachments.Exists(id, "test.png");
                    Assert.True(retiredExists);

                    var attachment = session.Advanced.Attachments.Get(id, "test.png");
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(RetiredAttachmentFlags.Retired, attachment.Details.RetireParameters.Flags);

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
                    new StoreAttachmentParameters("test.png", profileStream) { RetireParameters = new RetireAttachmentParameters(identifier, DateTime.UtcNow.AddMinutes(3)), ContentType = "image/png" });
                await session.SaveChangesAsync();
            }

            profileStream.Position = 0;

            Attachments.RemoveAll(x => x.DocumentId.ToLowerInvariant() == id && x.Name == name);

            Attachments.Add(new RetiredAttachment()
            {
                Name = name,
                DocumentId = id,
                Stream = profileStream,
                ContentType = "image/png"
            });
        }
    }
}
