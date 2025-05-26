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
            throw new NotImplementedException();
            //using (var store = GetDocumentStore())
            //{
            //    var c = new RetiredAttachmentsConfiguration()
            //    {
            //        S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
            //        Disabled = false,
            //        RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromMilliseconds(322228) } },
            //        RetireFrequencyInSec = 1000
            //    };

            //    await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(c));

            //    var config = await store.Maintenance.SendAsync(new GetRetireAttachmentsConfigurationOperation());
            //    Assert.Equal("testS3Bucket", config.S3Settings.BucketName);
            //    Assert.Equal(false, config.Disabled);
            //    var kvp1 = config.RetirePeriods.First(x => x.Key == "Orders");
            //    var kvp2 = config.RetirePeriods.First(x => x.Key == "Products");
            //    Assert.Equal("Orders", kvp1.Key);
            //    Assert.Equal(TimeSpan.FromDays(14), kvp1.Value);
            //    Assert.Equal("Products", kvp2.Key);
            //    Assert.Equal(TimeSpan.FromMilliseconds(322228), kvp2.Value);
            //    Assert.Equal(1000, config.RetireFrequencyInSec);
            //}
        }

        [RavenTheory(RavenTestCategory.Attachments)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task CanAssertRetiredAttachmentsConfiguration(bool disabled)
        {
            throw new NotImplementedException();
            //using (var store = GetDocumentStore())
            //{
            //    var e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
            //    {
            //        S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
            //        AzureSettings = new AzureSettings() { AccountName = "testAzureAccount", StorageContainer = "testAzureContainer" },
            //        Disabled = disabled,
            //        RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromDays(322) } },
            //        RetireFrequencyInSec = 1000
            //    })));
            //    Assert.Contains("Only one uploader for RetiredAttachmentsConfiguration can be configured", e.Message);
            //    e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
            //    {
            //        S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
            //        Disabled = disabled,
            //        RetireFrequencyInSec = 0
            //    })));
            //    Assert.Contains("Retire attachments frequency must be greater than 0.", e.Message);
            //    e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
            //    {
            //        S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
            //        Disabled = disabled,
            //        RetireFrequencyInSec = 1,
            //        MaxItemsToProcess = 0
            //    })));
            //    Assert.Contains("Max items to process must be greater than 0.", e.Message);
            //    e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
            //    {
            //        S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
            //        Disabled = disabled,
            //        RetireFrequencyInSec = 1,
            //        MaxItemsToProcess = 1
            //    })));
            //    Assert.Contains("RetirePeriods must have a value when Disabled is false.", e.Message);
            //    e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
            //    {
            //        S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
            //        Disabled = disabled,
            //        RetireFrequencyInSec = 1,
            //        MaxItemsToProcess = 1,
            //        RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromDays(-322) } }
            //    })));
            //    Assert.Contains("RetirePeriods must have positive TimeSpan values.", e.Message);

            //    e = await Assert.ThrowsAsync<InvalidOperationException>(async () => await store.Maintenance.SendAsync(new ConfigureRetiredAttachmentsOperation(new RetiredAttachmentsConfiguration()
            //    {
            //        S3Settings = new S3Settings() { BucketName = "testS3Bucket" },
            //        Disabled = disabled,
            //        MaxItemsToProcess = 1,
            //        RetirePeriods = new Dictionary<string, TimeSpan>() { { "Orders", TimeSpan.FromDays(14) }, { "Products", TimeSpan.FromDays(322) } }
            //    })));
            //    Assert.Contains($"RetireFrequencyInSec must have a value.", e.Message);
            //}
        }
    }
}
