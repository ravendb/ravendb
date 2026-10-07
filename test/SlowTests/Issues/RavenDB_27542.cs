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
    public async Task PutBlobShouldThrowWhenServiceSasTokenIsNotAllowedFromThisIp()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;

        var sasUri = GetContainerClient(settings).GenerateSasUri(new BlobSasBuilder(BlobContainerSasPermissions.All, DateTimeOffset.UtcNow.AddHours(1))
        {
            BlobContainerName = settings.StorageContainer.ToLower(),
            Resource = "c",
            IPRange = new SasIPRange(IPAddress.Parse("192.0.2.1")) // TEST-NET-1, never our address
        });

        // the premise: for a service SAS, Azure reports the IP restriction with the same error as the missing permission to read the container properties,
        // so the connection test can't detect it and the upload must report it
        var e = await Assert.ThrowsAsync<RequestFailedException>(() => new BlobContainerClient(sasUri).ExistsAsync());
        Assert.Equal(403, e.Status);
        Assert.Equal(BlobErrorCode.AuthorizationFailure.ToString(), e.ErrorCode);

        settings.AccountKey = null;
        settings.SasToken = sasUri.Query;

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            e = Assert.Throws<RequestFailedException>(() => client.PutBlob($"{Guid.NewGuid()}", new MemoryStream(Encoding.UTF8.GetBytes("123")), new Dictionary<string, string>()));
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

    [AzureRetryFact]
    public async Task TestConnectionShouldThrowOnServiceSasTokenWithWrongSignature()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;

        // signed with another key, like a token that was created before the account key was regenerated
        var sasToken = new BlobSasBuilder(BlobContainerSasPermissions.Create | BlobContainerSasPermissions.Write, DateTimeOffset.UtcNow.AddHours(1))
        {
            BlobContainerName = settings.StorageContainer.ToLower()
        }.ToSasQueryParameters(new StorageSharedKeyCredential(settings.AccountName, GetRandomAccountKey())).ToString();

        // the premise: Azure authenticates the token before it authorizes Get Container Properties, which a service SAS isn't allowed to do
        var e = await Assert.ThrowsAsync<RequestFailedException>(() => new BlobContainerClient(GetContainerUri(settings, sasToken)).ExistsAsync());
        Assert.Equal(403, e.Status);
        Assert.Equal(BlobErrorCode.AuthenticationFailed.ToString(), e.ErrorCode);

        await AssertTestConnectionThrowsAuthenticationFailed(settings, sasToken);
    }

    [AzureRetryFact]
    public async Task TestConnectionShouldThrowOnAccountSasTokenThatCannotReadContainerPropertiesWithWrongSignature()
    {
        var settings = AzureRetryFactAttribute.AzureSettings;

        var sasToken = new AccountSasBuilder(AccountSasPermissions.Create | AccountSasPermissions.Write,
                DateTimeOffset.UtcNow.AddHours(1), AccountSasServices.Blobs, AccountSasResourceTypes.Object)
            .ToSasQueryParameters(new StorageSharedKeyCredential(settings.AccountName, GetRandomAccountKey())).ToString();

        // the premise: Azure authenticates the token before it authorizes Get Container Properties, which this token isn't allowed to do
        var e = await Assert.ThrowsAsync<RequestFailedException>(() => new BlobContainerClient(GetContainerUri(settings, sasToken)).ExistsAsync());
        Assert.Equal(403, e.Status);
        Assert.Equal(BlobErrorCode.AuthenticationFailed.ToString(), e.ErrorCode);

        await AssertTestConnectionThrowsAuthenticationFailed(settings, sasToken);
    }

    [AzureRetryFact]
    public async Task TestConnectionAndPutBlobShouldSucceedWithSasTokenSurroundedByWhitespace()
    {
        using (var holder = new AzureClientHolder(AzureRetryFactAttribute.AzureSettings))
        {
            var sasUri = GetContainerClient(holder.Settings)
                .GenerateSasUri(BlobContainerSasPermissions.Create | BlobContainerSasPermissions.Write, DateTimeOffset.UtcNow.AddHours(1));

            await AssertTestConnectionAndPutBlobSucceed(holder, $" \t{sasUri.Query}\r\n");
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc")]
    [InlineData("SV=2025-01-05&SS=b&SRT=co&SP=rcw&SE=2099-01-01T00%3A00%3A00Z&SIG=abc")] // the parameter names are case insensitive
    public async Task TestConnectionShouldCheckTheContainerForAccountSasTokenThatCanReadContainerProperties(string sasToken)
    {
        // the account doesn't exist, so the check fails
        var settings = new AzureSettings
        {
            AccountName = $"nonexisting{Guid.NewGuid():N}"[..24],
            StorageContainer = "container",
            SasToken = sasToken
        };

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            await Assert.ThrowsAnyAsync<Exception>(() => client.TestConnectionAsync());
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("sv=2025-01-05&sr=c&sp=racwdl&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // service SAS
    [InlineData("sv=2025-01-05&sr=d&sdd=1&sp=racwdl&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // directory SAS
    [InlineData("sv=2025-01-05&sr=c&si=policy&sig=abc")] // stored access policy
    [InlineData("sv=2025-01-05&sr=c&sp=racwdl&skoid=00000000-0000-0000-0000-000000000000&sktid=00000000-0000-0000-0000-000000000000&skt=2026-01-01T00%3A00%3A00Z&ske=2099-01-01T00%3A00%3A00Z&sks=b&skv=2025-01-05&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // user delegation SAS
    [InlineData("sv=2025-01-05&ss=b&srt=o&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // account SAS without the container resource type
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=cw&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // account SAS without the read permission
    public async Task TestConnectionShouldReachAzureForSasTokenThatCannotReadContainerProperties(string sasToken)
    {
        // the account doesn't exist, so the request that checks the token fails
        var settings = new AzureSettings { AccountName = $"nonexisting{Guid.NewGuid():N}"[..24], StorageContainer = "container", SasToken = sasToken };

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            // the SDK retries and then throws all the attempts' failures
            var e = await Assert.ThrowsAsync<AggregateException>(() => client.TestConnectionAsync());
            Assert.All(e.InnerExceptions, inner => Assert.IsType<RequestFailedException>(inner));
        }
    }

    [RavenFact(RavenTestCategory.BackupExportImport)]
    public async Task TestConnectionShouldThrowForAccountSasTokenThatCannotUploadBlobs()
    {
        // the account doesn't exist, so this must fail before any request
        var settings = new AzureSettings
        {
            AccountName = $"nonexisting{Guid.NewGuid():N}"[..24],
            StorageContainer = "container",
            SasToken = "sv=2025-01-05&ss=b&srt=c&sp=rcwl&se=2099-01-01T00%3A00%3A00Z&sig=abc"
        };

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            var e = await Assert.ThrowsAsync<ArgumentException>(() => client.TestConnectionAsync());
            Assert.Contains("the 'srt' parameter must include 'o'", e.Message);
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // account SAS
    [InlineData("?sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // leading '?'
    [InlineData(" sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc\r\n")] // surrounding whitespace
    [InlineData("SV=2025-01-05&SS=b&SRT=c&SP=rl&SE=2099-01-01T00%3A00%3A00Z&SIG=abc")] // list only, used by the restore, the parameter names are case insensitive
    [InlineData("sv=2025-01-05&sr=c&sp=racwdl&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // service SAS
    [InlineData("sv=2025-01-05&sr=d&sdd=1&sp=racwdl&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // directory SAS
    [InlineData("sv=2025-01-05&sr=c&si=policy&sig=abc")] // stored access policy, which defines the permissions and the expiry time
    [InlineData("sv=2025-01-05&sr=c&sp=racwdl&skoid=00000000-0000-0000-0000-000000000000&sktid=00000000-0000-0000-0000-000000000000&skt=2026-01-01T00%3A00%3A00Z&ske=2099-01-01T00%3A00%3A00Z&sks=b&skv=2025-01-05&se=2099-01-01T00%3A00%3A00Z&sig=abc")] // user delegation SAS
    public void CreateShouldAcceptValidSasToken(string sasToken)
    {
        var settings = new AzureSettings { AccountName = $"nonexisting{Guid.NewGuid():N}"[..24], StorageContainer = "container", SasToken = sasToken };

        using (RavenAzureClient.Create(settings, DefaultConfiguration))
        {
        }
    }

    [RavenTheory(RavenTestCategory.BackupExportImport)]
    [InlineData("sv=2025-01-05&ss=b&srt&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "isn't in the correct format")] // a key without a value
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&se=tomorrow&sig=abc", "isn't in the correct format")] // invalid date
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&st=yesterday&se=2099-01-01T00%3A00%3A00Z&sig=abc", "isn't in the correct format")] // invalid date
    [InlineData("sv=2025-01-05&ss=b&srt=cox&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "isn't in the correct format")] // unknown resource type
    [InlineData("sv=2025-01-05&ss=bz&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "isn't in the correct format")] // unknown service
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&spr=ftp&se=2099-01-01T00%3A00%3A00Z&sig=abc", "isn't in the correct format")] // invalid protocol
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&sip=not-an-ip&se=2099-01-01T00%3A00%3A00Z&sig=abc", "isn't in the correct format")] // invalid IP
    [InlineData("sv=2025-01-05&ss=b&srt=o&srt=c&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "isn't in the correct format")] // the same key twice
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=a bc", "it contains whitespace")]
    [InlineData("https://account.blob.core.windows.net/container?sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "it's a SAS URL. Use only its query part")]
    [InlineData("BlobEndpoint=https://account.blob.core.windows.net/;SharedAccessSignature=sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "it's a connection string. Use only its SharedAccessSignature value")]
    [InlineData("\"sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc\"", "it's wrapped in quotes. Remove them")]
    [InlineData("ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "the 'sv' parameter is missing")]
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z", "the 'sig' parameter is missing")]
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&sig=abc", "the 'se' parameter is missing")]
    [InlineData("sv=2025-01-05&ss=b&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "the 'srt' parameter is missing")]
    [InlineData("sv=2025-01-05&ss=f&srt=co&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "the 'ss' parameter must include 'b'")]
    [InlineData("sv=2025-01-05&sr=b&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "but it has sr=b")]
    [InlineData("sv=2025-01-05&sr=bs&sp=rcw&se=2099-01-01T00%3A00%3A00Z&sig=abc", "but it has sr=bs")]
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&se=2020-01-01T00%3A00%3A00Z&sig=abc", "expired at")]
    [InlineData("sv=2025-01-05&sr=c&si=policy&se=2020-01-01T00%3A00%3A00Z&sig=abc", "expired at")] // the token's expiry time applies with a stored access policy too
    [InlineData("sv=2025-01-05&ss=b&srt=co&sp=rcw&st=2099-01-01T00%3A00%3A00Z&se=2099-01-02T00%3A00%3A00Z&sig=abc", "isn't valid before")]
    [InlineData("sv=2025-01-05&sr=c&sp=racwdl&skoid=00000000-0000-0000-0000-000000000000&sktid=00000000-0000-0000-0000-000000000000&skt=2020-01-01T00%3A00%3A00Z&ske=2020-01-02T00%3A00%3A00Z&sks=b&skv=2025-01-05&se=2099-01-01T00%3A00%3A00Z&sig=abc", "whose key expired at")]
    public void CreateShouldThrowOnInvalidSasToken(string sasToken, string expectedMessage)
    {
        var settings = new AzureSettings { AccountName = $"nonexisting{Guid.NewGuid():N}"[..24], StorageContainer = "container", SasToken = sasToken };

        var e = Assert.Throws<ArgumentException>(() => RavenAzureClient.Create(settings, DefaultConfiguration));
        Assert.Contains(nameof(AzureSettings.SasToken), e.Message);
        Assert.Contains(expectedMessage, e.Message);
    }

    private static async Task AssertTestConnectionThrowsAuthenticationFailed(AzureSettings settings, string sasToken)
    {
        settings.AccountKey = null;
        settings.SasToken = sasToken;

        using (var client = RavenAzureClient.Create(settings, DefaultConfiguration))
        {
            var e = await Assert.ThrowsAsync<RequestFailedException>(() => client.TestConnectionAsync());
            Assert.Equal(403, e.Status);
            Assert.Equal(BlobErrorCode.AuthenticationFailed.ToString(), e.ErrorCode);
        }
    }

    private static string GetRandomAccountKey()
    {
        return Convert.ToBase64String(Guid.NewGuid().ToByteArray());
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
