using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Raven.Server.Documents.PeriodicBackup.Azure;
using SlowTests.Server.Documents.PeriodicBackup;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;
using AzureClientHolder = SlowTests.Server.Documents.PeriodicBackup.Azure.AzureClientHolder;

namespace SlowTests.Issues;

public class RavenDB_27542 : CloudBackupTestBase
{
    public RavenDB_27542(ITestOutputHelper output) : base(output)
    {
    }

    [AzureRetryFact]
    public async Task TestConnectionAndPutBlobShouldSucceedWithSasTokenThatCannotReadContainerProperties()
    {
        // the holder uses the account key, so it can read back and clean up what the write-only client uploads
        using (var holder = new AzureClientHolder(AzureRetryFactAttribute.AzureSettings))
        {
            var sasUri = GetContainerClient(holder.Settings.AccountName, holder.Settings.AccountKey, holder.Settings.StorageContainer)
                .GenerateSasUri(BlobContainerSasPermissions.Create | BlobContainerSasPermissions.Write, DateTimeOffset.UtcNow.AddHours(1));

            // the premise: a write-only SAS token isn't allowed to check if the container exists
            var e = await Assert.ThrowsAsync<RequestFailedException>(() => new BlobContainerClient(sasUri).ExistsAsync());
            Assert.Equal(403, e.Status);
            Assert.Contains(e.ErrorCode, new[] { BlobErrorCode.AuthorizationPermissionMismatch.ToString(), BlobErrorCode.AuthorizationResourceTypeMismatch.ToString() });

            var sasSettings = AzureRetryFactAttribute.AzureSettings;
            sasSettings.AccountKey = null;
            sasSettings.SasToken = sasUri.Query.TrimStart('?');
            sasSettings.RemoteFolderName = holder.Settings.RemoteFolderName;

            var blobName = $"{sasSettings.RemoteFolderName}/{Guid.NewGuid()}";

            using (var sasClient = RavenAzureClient.Create(sasSettings, DefaultConfiguration))
            {
                await sasClient.TestConnectionAsync();

                sasClient.PutBlob(blobName, new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>());
            }

            var blob = await holder.Client.GetBlobAsync(blobName);
            using (var reader = new StreamReader(blob.Data))
                Assert.Equal("123", await reader.ReadToEndAsync());
        }
    }

    [AzureRetryFact]
    public async Task TestConnectionShouldThrowOnInvalidAccountKey()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;
        settings.AccountKey = Convert.ToBase64String(Guid.NewGuid().ToByteArray());

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            var e = await Assert.ThrowsAsync<RequestFailedException>(() => client.TestConnectionAsync());
            Assert.Equal(403, e.Status);
            Assert.Equal(BlobErrorCode.AuthenticationFailed.ToString(), e.ErrorCode);
        }
    }

    [AzureRetryFact]
    public async Task TestConnectionShouldThrowWhenSasTokenIsNotAllowedFromThisIp()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;

        // a token with full permissions that can't be used from here at all, must not be reported as a successful connection
        var sasBuilder = new BlobSasBuilder(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddHours(1))
        {
            BlobContainerName = settings.StorageContainer.ToLower(),
            Resource = "c",
            IPRange = new SasIPRange(IPAddress.Parse("192.0.2.1")) // TEST-NET-1, never our address
        };

        var sasUri = GetContainerClient(settings.AccountName, settings.AccountKey, settings.StorageContainer).GenerateSasUri(sasBuilder);

        settings.AccountKey = null;
        settings.SasToken = sasUri.Query.TrimStart('?');

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            var e = await Assert.ThrowsAsync<RequestFailedException>(() => client.TestConnectionAsync());
            Assert.Equal(403, e.Status);
            Assert.Equal(BlobErrorCode.AuthorizationSourceIPMismatch.ToString(), e.ErrorCode);
        }
    }

    [AzureRetryFact]
    public async Task TestConnectionShouldThrowWhenContainerDoesNotExist()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;
        settings.StorageContainer = $"missing-{Guid.NewGuid():N}";

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            await Assert.ThrowsAsync<ContainerNotFoundException>(() => client.TestConnectionAsync());
        }
    }

    private static BlobContainerClient GetContainerClient(string accountName, string accountKey, string container)
    {
        var uri = new Uri($"https://{accountName}.blob.core.windows.net/{container.ToLower()}", UriKind.Absolute);
        return new BlobContainerClient(uri, new StorageSharedKeyCredential(accountName, accountKey));
    }
}
