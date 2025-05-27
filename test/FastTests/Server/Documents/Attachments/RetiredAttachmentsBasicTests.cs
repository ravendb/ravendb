using System;
using System.Threading.Tasks;
using Raven.Client.Documents.Attachments;
using Raven.Client.Documents.Operations.Attachments.Retired;
using Raven.Client.Documents.Operations.Backups;
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
                    RetireFrequencyInSec = 1000
                };

                await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(c));

                var config = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
                Assert.Equal("testS3Bucket", config.S3Settings.BucketName);
                Assert.Equal(false, config.Disabled);
                Assert.Equal(1000, config.RetireFrequencyInSec);
            }
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanAssertRetiredAttachmentsConfiguration(bool disabled)
        {
            using (var store = GetDocumentStore())
            {
                var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                    Disabled = disabled,
                    RetireFrequencyInSec = 1000
                })));
                Assert.Contains("Only one uploader for RetiredAttachmentsConfiguration can be configured", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = disabled,
                    RetireFrequencyInSec = 0
                })));
                Assert.Contains("Retire attachments frequency must be greater than 0.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = disabled,
                    RetireFrequencyInSec = 1,
                    MaxItemsToProcess = 0
                })));
                Assert.Contains("Max items to process must be greater than 0.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    Disabled = disabled,
                    MaxItemsToProcess = 0
                })));
                Assert.Contains("RetireFrequencyInSec must have a value.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    Disabled = disabled,
                    MaxItemsToProcess = 1,
                    RetireFrequencyInSec = 1,
                })));
                Assert.Contains("Exactly one uploader for RetiredAttachmentsConfiguration must be configured.", e.Message);
                e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
                {
                    S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
                    AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
                    Disabled = disabled,
                    MaxItemsToProcess = 1,
                    RetireFrequencyInSec = 1,
                })));
                Assert.Contains("Only one uploader for RetiredAttachmentsConfiguration can be configured.", e.Message);
            }
        }
    }
}
