using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Orders;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Attachments
{
    public class DocumentSessionRetiredAttachmentsTests : RetiredAttachmentsS3Base
    {
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                store.Operations.Send(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

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
                    Assert.Throws<InvalidOperationException>(() => session.Advanced.Attachments.Exists(id, "test.png"));

                    var retiredExists = session.Advanced.RetiredAttachments.Exists(id, "test.png");
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                store.Operations.Send(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var attachment = session.Advanced.RetiredAttachments.Get(id, "test.png");
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                store.Operations.Send(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));



                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    var attachment = session.Advanced.RetiredAttachments.Get(order, "test.png");
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
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/4", Company = $"Companies/4" });
                    session.SaveChanges();
                }

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                store.Operations.Send(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenSession())
                {
                    var attachments = session.Advanced.RetiredAttachments.Get(new List<AttachmentRequest> { new AttachmentRequest(id, "test.png") });
                    Assert.NotNull(attachments);
                    Assert.True(attachments.MoveNext());
                    var attachment = attachments.Current;
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndName(bool storageOnly)
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                store.Operations.Send(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));


                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                using (var session = store.OpenSession())
                {
                    session.Advanced.RetiredAttachments.Delete(id, "test.png", storageOnly);
                    session.SaveChanges();

                    var exists = session.Advanced.RetiredAttachments.Exists(id, "test.png");
                    Assert.False(exists);
                }

                if (storageOnly)
                {
                    await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                }
                else
                {
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDeleteRetiredAttachmentByEntityAndName(bool storageOnly)
        {
            await using (var holder = CreateCloudSettings())
            using (var store = GetDocumentStore())
            {
                await PutRetireAttachmentsConfiguration(store, Settings, collections: null);
                var id = "Orders/6";
                using (var session = store.OpenSession())
                {
                    var order = new Order { Id = id, OrderedAt = new DateTime(2024, 1, 1), ShipVia = $"Shippers/6", Company = $"Companies/6" };
                    session.Store(order);
                    session.SaveChanges();
                }

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                store.Operations.Send(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                await GetBlobsFromCloudAndAssertForCount(Settings, 1);

                using (var session = store.OpenSession())
                {
                    var order = session.Load<Order>(id);
                    session.Advanced.RetiredAttachments.Delete(order, "test.png", storageOnly);
                    session.SaveChanges();

                    var exists = session.Advanced.RetiredAttachments.Exists(id, "test.png");
                    Assert.False(exists);
                }

                if (storageOnly)
                {
                    await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                }
                else
                {
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                }
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
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

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
                    await Assert.ThrowsAsync<InvalidOperationException>(async () => await session.Advanced.Attachments.ExistsAsync(id, "test.png"));

                    var retiredExists = await session.Advanced.RetiredAttachments.ExistsAsync(id, "test.png");
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var attachment = await session.Advanced.RetiredAttachments.GetAsync(id, "test.png");
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));


                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    var attachment = await session.Advanced.RetiredAttachments.GetAsync(order, "test.png");
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var attachments = await session.Advanced.RetiredAttachments.GetAsync(new List<AttachmentRequest> { new AttachmentRequest(id, "test.png") });
                    Assert.NotNull(attachments);
                    //TODO: egor think if we can return async enumerator
                    //Assert.True(await attachments.MoveNextAsync());
                    Assert.True(attachments.MoveNext());
                    var attachment = attachments.Current;
                    Assert.NotNull(attachment);
                    Assert.Equal("test.png", attachment.Details.Name);
                    Assert.Equal(AttachmentFlags.Retired, attachment.Details.Flags);
                }
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDeleteRetiredAttachmentByDocumentIdAndNameAsync(bool storageOnly)
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    await session.Advanced.RetiredAttachments.DeleteAsync(id, "test.png", storageOnly);
                    await session.SaveChangesAsync();

                    var exists = await session.Advanced.RetiredAttachments.ExistsAsync(id, "test.png");
                    Assert.False(exists);
                }

                if (storageOnly)
                {
                    await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                }
                else
                {
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                }
            }
        }


        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanDeleteRetiredAttachmentByEntityAndNameAsync(bool storageOnly)
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

                using var profileStream = new MemoryStream(new byte[] { 1, 2, 3 });
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var database = await Databases.GetDocumentDatabaseInstanceFor(Server, store);
                database.Time.UtcDateTime = () => DateTime.UtcNow.AddMinutes(10);
                await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);

                using (var session = store.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<Order>(id);
                    await session.Advanced.RetiredAttachments.DeleteAsync(order, "test.png", storageOnly);
                    await session.SaveChangesAsync();

                    var exists = await session.Advanced.RetiredAttachments.ExistsAsync(id, "test.png");
                    Assert.False(exists);
                }


                if (storageOnly)
                {
                    await GetBlobsFromCloudAndAssertForCount(Settings, 1);
                }
                else
                {
                    await database.RetireAttachmentsSender.RetireAttachments(int.MaxValue, int.MaxValue);
                    await GetBlobsFromCloudAndAssertForCount(Settings, 0);
                }
            }
        }
    }
}
