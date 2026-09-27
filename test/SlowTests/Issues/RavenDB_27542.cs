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
using Raven.Client.Documents.Operations.Backups;
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
    public async Task TestConnectionAndPutBlobShouldSucceedWithServiceSasToken()
    {
        // the holder uses the account key, so it can read back and clean up what the SAS client uploads
        using (var holder = new AzureClientHolder(AzureRetryFactAttribute.AzureSettings))
        {
            var sasUri = GetContainerClient(holder.Settings)
                .GenerateSasUri(BlobContainerSasPermissions.Create | BlobContainerSasPermissions.Write, DateTimeOffset.UtcNow.AddHours(1));

            // the premise: a service SAS can't read the container properties, whatever its permissions are
            var e = await Assert.ThrowsAsync<RequestFailedException>(() => new BlobContainerClient(sasUri).ExistsAsync());
            Assert.Equal(403, e.Status);
            Assert.Equal(BlobErrorCode.AuthorizationFailure.ToString(), e.ErrorCode);

            await AssertTestConnectionAndPutBlobSucceed(holder, sasUri.Query.TrimStart('?'));
        }
    }

    [AzureRetryFact]
    public async Task TestConnectionAndPutBlobShouldSucceedWithAccountSasTokenThatCannotReadContainerProperties()
    {
        using (var holder = new AzureClientHolder(AzureRetryFactAttribute.AzureSettings))
        {
            // object level write only, Get Container Properties requires the container resource type and the read permission
            var sasToken = GetAccountSasToken(holder.Settings, new AccountSasBuilder(AccountSasPermissions.Create | AccountSasPermissions.Write,
                DateTimeOffset.UtcNow.AddHours(1), AccountSasServices.Blobs, AccountSasResourceTypes.Object));

            // the premise: this token isn't allowed to check if the container exists
            var e = await Assert.ThrowsAsync<RequestFailedException>(() => new BlobContainerClient(GetContainerUri(holder.Settings, sasToken)).ExistsAsync());
            Assert.Equal(403, e.Status);

            await AssertTestConnectionAndPutBlobSucceed(holder, sasToken);
        }
    }

    [AzureRetryFact]
    public async Task TestConnectionShouldThrowWhenAccountSasTokenIsNotAllowedFromThisIp()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;

        // this token can read the container properties, so the check runs and the IP restriction must be reported
        settings.SasToken = GetAccountSasToken(settings, new AccountSasBuilder(AccountSasPermissions.Read | AccountSasPermissions.Create | AccountSasPermissions.Write,
            DateTimeOffset.UtcNow.AddHours(1), AccountSasServices.Blobs, AccountSasResourceTypes.Container | AccountSasResourceTypes.Object)
        {
            IPRange = new SasIPRange(IPAddress.Parse("192.0.2.1")) // TEST-NET-1, never our address
        });
        settings.AccountKey = null;

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            var e = await Assert.ThrowsAsync<RequestFailedException>(() => client.TestConnectionAsync());
            Assert.Equal(403, e.Status);
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
    public async Task TestConnectionShouldThrowWhenContainerDoesNotExist()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;
        settings.StorageContainer = $"missing-{Guid.NewGuid():N}";

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            await Assert.ThrowsAsync<ContainerNotFoundException>(() => client.TestConnectionAsync());
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("sv=2025-01-05&sr=c&sp=racwdl&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // service SAS
    [InlineData("?sv=2025-01-05&sr=c&sp=racwdl&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // service SAS with a leading '?'
    [InlineData("sv=2025-01-05&sr=c&sp=racwdl&skoid=00000000-0000-0000-0000-000000000000&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // user delegation SAS
    [InlineData("sv=2025-01-05&ss=b&srt=o&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // account SAS without the container resource type
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=cw&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // account SAS without the read permission
    public async Task TestConnectionShouldSkipTheContainerCheckForSasTokenThatCannotReadContainerProperties(string sasToken)
    {
        // the account doesn't exist, so any request would fail
        var settings = new AzureSettings { AccountName = $"nonexisting{Guid.NewGuid():N}"[..24], StorageContainer = "container", SasToken = sasToken };

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            await client.TestConnectionAsync();
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task TestConnectionShouldCheckTheContainerForAccountSasTokenThatCanReadContainerProperties()
    {
        // the account doesn't exist, so the check fails
        var settings = new AzureSettings
        {
            AccountName = $"nonexisting{Guid.NewGuid():N}"[..24],
            StorageContainer = "container",
            SasToken = "sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc"
        };

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            await Assert.ThrowsAnyAsync<Exception>(() => client.TestConnectionAsync());
        }
    }

    private static async Task AssertTestConnectionAndPutBlobSucceed(AzureClientHolder holder, string sasToken)
    {
        var sasSettings = AzureRetryFactAttribute.AzureSettings;
        sasSettings.AccountKey = null;
        sasSettings.SasToken = sasToken;
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

    private static string GetAccountSasToken(AzureSettings settings, AccountSasBuilder builder)
    {
        return builder.ToSasQueryParameters(new StorageSharedKeyCredential(settings.AccountName, settings.AccountKey)).ToString();
    }

    private static Uri GetContainerUri(AzureSettings settings, string sasToken = null)
    {
        var uri = $"https://{settings.AccountName}.blob.core.windows.net/{settings.StorageContainer.ToLower()}";
        return new Uri(sasToken == null ? uri : $"{uri}?{sasToken}", UriKind.Absolute);
    }

    private static BlobContainerClient GetContainerClient(AzureSettings settings)
    {
        return new BlobContainerClient(GetContainerUri(settings), new StorageSharedKeyCredential(settings.AccountName, settings.AccountKey));
    }
}
