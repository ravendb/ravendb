using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using FastTests.Client;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Exceptions;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Server.Documents.Attachments
{
    public class RetiredAttachmentsBasicTests : RavenTestBase
    {
        public RetiredAttachmentsBasicTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutAndGetRetiredAttachmentsConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var c = new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = false,
                    RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromMilliseconds(322228) } },
                    RetireFrequencyInSec = 1000
                };

                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(c));

                var config = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
                Assert.Equal("testS3Bucket", config.S3Settings.BucketName);
                Assert.Equal(false, config.Disabled);
                var kvp1 = config.RetirePeriods.First(x => x.Key == "Orders");
                var kvp2 = config.RetirePeriods.First(x => x.Key == "Products");
                Assert.Equal("Orders", kvp1.Key);
                Assert.Equal(TimeSpan.FromDays(14), kvp1.Value);
                Assert.Equal("Products", kvp2.Key);
                Assert.Equal(TimeSpan.FromMilliseconds(322228), kvp2.Value);
                Assert.Equal(1000, config.RetireFrequencyInSec);
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanAssertRetiredAttachmentsConfiguration()
        {
            using (var store = GetDocumentStore())
            {
                var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                    Disabled = false,
                    RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromDays(322) } },
                    RetireFrequencyInSec = 1000
                })));
                Assert.Contains("Only one uploader for RetiredAttachmentsConfiguration can be configured when Disabled is false.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = false,
                    RetireFrequencyInSec = 0
                })));
                Assert.Contains("Retire attachments frequency must be greater than 0.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = false,
                    RetireFrequencyInSec = 1,
                    MaxItemsToProcess = 0
                })));
                Assert.Contains("Max items to process must be greater than 0.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = false,
                    RetireFrequencyInSec = 1,
                    MaxItemsToProcess = 1
                })));
                Assert.Contains("RetirePeriods must have a value when Disabled is false.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = false,
                    RetireFrequencyInSec = 1,
                    MaxItemsToProcess = 1,
                    RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromDays(-322) } }
                })));
                Assert.Contains("RetirePeriods must have positive TimeSpan values.", e.Message);
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task ShouldThrowUsingRetiredAttachmentsApiOnRegularAttachment()
        {
            using (var store = GetDocumentStore())
            {
                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Query.Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var res = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.Equal("test.png", res.Details.Name);

                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new GetRetiredAttachmentOperation(id, "test.png")));
                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new DeleteRetiredAttachmentOperation(id, "test.png")));
                await Assert.ThrowsAsync(typeof(RavenException),
                    async () => await store.Operations.SendAsync(new GetRetiredAttachmentsOperation(new List<AttachmentRequest> { new(id, "test.png") })));
            }
        }

        [RavenFact(RavenTestCategory.Attachments)]
        public async Task CanPutAttachmentThenAddRetiredConfigAndNewAttachment()
        {
            using (var store = GetDocumentStore())
            {
                var id = "Orders/3";
                using (var session = store.OpenAsyncSession())
                {
                    await session.StoreAsync(new Query.Order
                    {
                        Id = id,
                        OrderedAt = new DateTime(2024, 1, 1),
                        ShipVia = $"Shippers/2",
                        Company = $"Companies/2"
                    });

                    await session.SaveChangesAsync();
                }

                using var profileStream = new MemoryStream([1, 2, 3]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test.png", profileStream, "image/png"));

                var res = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.Equal("test.png", res.Details.Name);
                Assert.Equal(AttachmentFlags.None, res.Details.Flags);
                Assert.Null(res.Details.RetireAt);

                var c = new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = false,
                    RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromMilliseconds(322228) } },
                    RetireFrequencyInSec = 1000
                };

                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(c));


                using var profileStream2 = new MemoryStream([3,2,1]);
                await store.Operations.SendAsync(new PutAttachmentOperation(id, "test2.png", profileStream2, "image/png"));

                var res2 = await store.Operations.SendAsync(new GetAttachmentOperation(id, "test2.png", AttachmentType.Document, null));
                Assert.Equal("test2.png", res2.Details.Name);

                Assert.Equal(AttachmentFlags.None, res2.Details.Flags);
                Assert.NotNull(res2.Details.RetireAt);

                var res3= await store.Operations.SendAsync(new GetAttachmentOperation(id, "test.png", AttachmentType.Document, null));
                Assert.Equal("test.png", res3.Details.Name);
                Assert.Equal(AttachmentFlags.None, res3.Details.Flags);


                // TODO: what should happen? shouldn't this have retireAt to as well, since I added retired config...
                // marked as notnull for now so the test fails
                Assert.NotNull(res3.Details.RetireAt);
            }
        }

    }
}
