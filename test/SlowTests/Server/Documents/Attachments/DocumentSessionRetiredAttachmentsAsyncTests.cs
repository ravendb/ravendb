using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class DocumentSessionRetiredAttachmentsTests : RetiredAttachmentsS3Base
    {
        //TODO: egor add cluster wide session test
        public DocumentSessionRetiredAttachmentsTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanCheckIfRetiredAttachmentExists()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                        RetireAt = DateTime.UtcNow.AddMinutes(3),
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

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanGetRetiredAttachmentByDocumentIdAndName()
        {
            using (var store = GetDocumentStore())
            await using (var holder = CreateCloudSettings())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/2";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });
                    session.SaveChanges();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanGetRetiredAttachmentByEntityAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanGetEnumeratorOfRetiredAttachments()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanDeleteRetiredAttachmentByEntityAndName()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/6";

                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/6", Company = $"Companies/6" });
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanCheckIfRetiredAttachmentExistsAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                        new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
                  await  session.SaveChangesAsync();
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

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanGetRetiredAttachmentByDocumentIdAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/2";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/2", Company = $"Companies/2" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanGetRetiredAttachmentByEntityAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanGetEnumeratorOfRetiredAttachmentsAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/4";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/5";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/5", Company = $"Companies/5" });
                    await session.SaveChangesAsync();
                }

                using (var session = store.OpenAsyncSession())
                {
                    using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.Attachments.DeleteAsync(id, "test.png");
                    await session.SaveChangesAsync();

                    var exists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.False(exists);
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }


        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanDeleteRetiredAttachmentByEntityAndNameAsync()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
                    await session.SaveChangesAsync();
                }

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    await session.Advanced.Attachments.DeleteAsync(order, "test.png");
                    await session.SaveChangesAsync();

                    var exists = await session.Advanced.Attachments.ExistsAsync(id, "test.png");
                    Assert.False(exists);
                }

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(new byte[] { 1, 2, 3 })]
        [InlineData(new byte[] { 3, 2, 1 })]
        public async Task CanOverwriteRetireAttachment(byte[] buffer)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/1";
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/1", Company = $"Companies/1" });
                    session.SaveChanges();
                }

                var buf = new byte[] { 1, 2, 3 };
                using var profileStream = new MemoryStream(buf);
                await PutAttachmentForTests(store, id, "test.png", profileStream, "Orders");
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", profileStream2) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndNameAndRead()
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
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
                        new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    session.Advanced.Attachments.Store(id, new StoreAttachmentParameters("test.png", newProfileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
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
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);

                    // compare streams
                    using var ms = new MemoryStream();
                    await attachment.Stream.CopyToAsync(ms);
                    Assert.Equal(b, ms.ToArray());

                    await GetBlobsFromCloudAndAssertForCount(Settings, 2);
                }
            }
        }

        private async Task PutAttachmentForTests(DocumentStore store, string id, string name, MemoryStream profileStream, string collection)
        {
            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Attachments.Store(id,
                    new StoreAttachmentParameters("test.png", profileStream) { RetireAt = DateTime.UtcNow.AddMinutes(3), ContentType = "image/png" });
                await session.SaveChangesAsync();
            }

            profileStream.Position = 0;

            Attachments.RemoveAll(x => x.DocumentId.ToLowerInvariant() == id && x.Name == name);

            Attachments.Add(new RetiredAttachment()
            {
                Name = name,
                DocumentId = id,
                Collection = collection,
                Stream = profileStream,
                ContentType = "image/png"
            });
        }
    }
}
