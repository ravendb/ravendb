using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Azure;
using Azure.Storage;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.DirectUpload;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Sparrow;
using BackupConfiguration = Raven.Server.Config.Categories.BackupConfiguration;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup.Azure
{
    public interface IRavenAzureClient : IDirectUploader, IDisposable
    {
        void PutBlob(string blobName, Stream stream, Dictionary<string, string> metadata);
        RavenStorageClient.ListBlobResult ListBlobs(string prefix, string delimiter, bool listFolders, string continuationToken = null);
        Task<RavenStorageClient.ListBlobResult> ListBlobsAsync(string prefix, string delimiter, bool listFolders, string continuationToken = null);
        Task<RavenStorageClient.Blob> GetBlobAsync(string blobName);
        void DeleteBlobs(List<string> blobsToDelete);
        Task TestConnectionAsync();
        string RemoteFolderName { get; }
        Size MaxUploadPutBlob { get; set; }
        Size MaxSingleBlockSize { get; set; }
    }

    public sealed class RavenAzureClient : IProgress<long>, IRavenAzureClient
    {
        private readonly Progress _progress;

        private readonly CancellationToken _cancellationToken;
        private readonly BlobContainerClient _client;
        private readonly BlobSasQueryParameters _sas;
        private readonly bool _canReadContainerProperties;

        private static readonly TimeSpan SasTokenClockSkew = TimeSpan.FromMinutes(15);

        public string RemoteFolderName { get; }
        private readonly string _storageContainer;

        private static readonly Size TotalBlocksSizeLimit = new Size(4750, SizeUnit.Gigabytes);

        public Size MaxUploadPutBlob { get; set; } = new Size(256, SizeUnit.Megabytes);

        public Size MaxSingleBlockSize { get; set; } = new Size(256, SizeUnit.Megabytes);

        private RavenAzureClient(AzureSettings azureSettings, BackupConfiguration configuration, Progress progress = null, CancellationToken cancellationToken = default)
        {
            if (azureSettings == null)
                throw new ArgumentNullException(nameof(azureSettings));
            if (configuration == null)
                throw new ArgumentNullException(nameof(configuration));

            var hasAccountKey = string.IsNullOrWhiteSpace(azureSettings.AccountKey) == false;
            var hasSasToken = string.IsNullOrWhiteSpace(azureSettings.SasToken) == false;

            if (hasAccountKey == false && hasSasToken == false)
                throw new ArgumentException($"{nameof(AzureSettings.AccountKey)} and {nameof(AzureSettings.SasToken)} cannot be both null or empty");

            if (hasAccountKey && hasSasToken)
                throw new ArgumentException($"{nameof(AzureSettings.AccountKey)} and {nameof(AzureSettings.SasToken)} cannot be used simultaneously");

            if (string.IsNullOrWhiteSpace(azureSettings.AccountName))
                throw new ArgumentException($"{nameof(AzureSettings.AccountName)} cannot be null or empty");

            if (string.IsNullOrWhiteSpace(azureSettings.StorageContainer))
                throw new ArgumentException($"{nameof(AzureSettings.StorageContainer)} cannot be null or empty");

            var serverUrlForContainer = new Uri($"https://{azureSettings.AccountName}.blob.core.windows.net/{azureSettings.StorageContainer.ToLower()}", UriKind.Absolute);

            var options = new BlobClientOptions
            {
                Retry =
                {
                    NetworkTimeout = configuration.CloudStorageOperationTimeout.AsTimeSpan
                }
            };

            if (hasAccountKey)
            {
                _client = new BlobContainerClient(serverUrlForContainer, new StorageSharedKeyCredential(azureSettings.AccountName, azureSettings.AccountKey), options);
                _canReadContainerProperties = true;
            }

            if (hasSasToken)
            {
                var sasToken = azureSettings.SasToken.Trim().TrimStart('?');
                VerifySasToken(sasToken);
                _sas = ParseSasToken(serverUrlForContainer, sasToken);
                _client = new BlobContainerClient(serverUrlForContainer, new AzureSasCredential(sasToken), options);
                _canReadContainerProperties = CanReadContainerProperties(_sas);
            }

            _progress = progress;
            _cancellationToken = cancellationToken;
            _storageContainer = azureSettings.StorageContainer;
            RemoteFolderName = azureSettings.RemoteFolderName;
        }

        public void PutBlob(string blobName, Stream stream, Dictionary<string, string> metadata)
        {
            AsyncHelpers.RunSync(TestConnectionAsync);

            var streamSize = new Size(stream.Length, SizeUnit.Bytes);
            if (streamSize > TotalBlocksSizeLimit)
                throw new InvalidOperationException(@"Can't upload more than 4.75TB to Azure, " +
                                                    $"current upload size: {streamSize}");

            var streamLength = streamSize.GetValue(SizeUnit.Bytes);

            try
            {
                _progress?.UploadProgress.SetTotal(streamLength);

                var maxSingleBlockSizeInBytes = MaxSingleBlockSize.GetValue(SizeUnit.Bytes);
                if (streamLength > maxSingleBlockSizeInBytes)
                    _progress?.UploadProgress.ChangeType(UploadType.Chunked);

                var client = _client.GetBlobClient(blobName);

                client.Upload(stream, metadata: metadata, progressHandler: this, transferOptions: new StorageTransferOptions
                {
                    MaximumTransferSize = maxSingleBlockSizeInBytes
                }, cancellationToken: _cancellationToken);
            }
            finally
            {
                _progress?.UploadProgress.ChangeState(UploadState.Done);
            }
        }

        public RavenStorageClient.ListBlobResult ListBlobs(string prefix, string delimiter, bool listFolders, string continuationToken = null)
        {
            var pageable = _client.GetBlobsByHierarchy(BlobTraits.None, BlobStates.None, prefix: prefix, delimiter: delimiter, cancellationToken: _cancellationToken);
            var pages = pageable.AsPages(continuationToken: continuationToken);

            var result = new RavenStorageClient.ListBlobResult();

            using (var enumerator = pages.GetEnumerator())
            {
                while (enumerator.MoveNext())
                {
                    FillResult(result, enumerator.Current, listFolders);
                    break;
                }
            }

            return result;
        }

        public async Task<RavenStorageClient.ListBlobResult> ListBlobsAsync(string prefix, string delimiter, bool listFolders, string continuationToken = null)
        {
            var pageable = _client.GetBlobsByHierarchyAsync(BlobTraits.None, BlobStates.None, prefix: prefix, delimiter: delimiter, cancellationToken: _cancellationToken);
            var pages = pageable.AsPages(continuationToken: continuationToken);

            var result = new RavenStorageClient.ListBlobResult();

            await using (var enumerator = pages.GetAsyncEnumerator(cancellationToken: _cancellationToken))
            {
                while (await enumerator.MoveNextAsync())
                {
                    FillResult(result, enumerator.Current, listFolders);
                    break;
                }
            }

            return result;
        }

        public async Task<RavenStorageClient.Blob> GetBlobAsync(string blobName)
        {
            BlobClient blob = _client.GetBlobClient(blobName);

            var properties = await blob.GetPropertiesAsync(cancellationToken: _cancellationToken);
            var response = await blob.DownloadAsync(cancellationToken: _cancellationToken);

            return new RavenStorageClient.Blob(response.Value.Content, properties.Value.Metadata, response.Value.ContentLength, response.Value);
        }

        public void DeleteBlobs(List<string> blobsToDelete)
        {
            if (blobsToDelete == null || blobsToDelete.Count == 0)
                return;

            foreach (string blobName in blobsToDelete)
            {
                _client.DeleteBlobIfExists(blobName, cancellationToken: _cancellationToken);
            }
        }

        public async Task TestConnectionAsync()
        {
            if (_sas != null && string.IsNullOrEmpty(_sas.Resource) && _sas.ResourceTypes?.HasFlag(AccountSasResourceTypes.Object) != true)
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} can't be used to upload blobs, the 'srt' parameter must include 'o'");

            try
            {
                if (await _client.ExistsAsync(cancellationToken: _cancellationToken) == false)
                    throw new ContainerNotFoundException($"Container '{_storageContainer}' wasn't found!");
            }
            catch (RequestFailedException e) when (_canReadContainerProperties == false && IsAuthorizationFailure(e))
            {
            }
        }

        private static bool IsAuthorizationFailure(RequestFailedException e)
        {
            // report the errors that fail every request, not only Get Container Properties
            return e.Status == 403 &&
                   e.ErrorCode != BlobErrorCode.AuthenticationFailed.ToString() &&
                   e.ErrorCode != BlobErrorCode.AuthorizationSourceIPMismatch.ToString() &&
                   e.ErrorCode != BlobErrorCode.AccountIsDisabled.ToString();
        }

        private static bool CanReadContainerProperties(BlobSasQueryParameters sas)
        {
            // a service / user delegation SAS (has 'sr') can't read container properties at all,
            // an account SAS needs srt=c and sp=r
            // https://learn.microsoft.com/en-us/rest/api/storageservices/create-account-sas#blob-service
            return string.IsNullOrEmpty(sas.Resource) &&
                   sas.ResourceTypes?.HasFlag(AccountSasResourceTypes.Container) == true &&
                   sas.Permissions?.Contains('r') == true;
        }

        private static BlobSasQueryParameters ParseSasToken(Uri containerUri, string sasToken)
        {
            if (sasToken.Any(char.IsWhiteSpace))
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format, it contains whitespace");

            BlobSasQueryParameters sas;
            try
            {
                sas = new BlobUriBuilder(new UriBuilder(containerUri) { Query = sasToken }.Uri).Sas;
            }
            catch (Exception e)
            {
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format", e);
            }

            if (sas == null)
                throw new ArgumentException(GetUnrecognizedSasTokenMessage(sasToken));

            if (string.IsNullOrEmpty(sas.Signature))
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format, the 'sig' parameter is missing");

            if (string.IsNullOrEmpty(sas.Resource))
            {
                if (sas.Services?.HasFlag(AccountSasServices.Blobs) != true)
                    throw new ArgumentException($"{nameof(AzureSettings.SasToken)} doesn't grant access to the Blob service, the 'ss' parameter must include 'b'");

                if (sas.ResourceTypes == null)
                    throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format, the 'srt' parameter is missing");
            }
            else if (sas.Resource != "c" && sas.Resource != "d")
            {
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} must be scoped to a container (sr=c) or a directory (sr=d), but it has sr={sas.Resource}");
            }

            var now = DateTimeOffset.UtcNow;

            if (sas.ExpiresOn == default)
            {
                // a stored access policy ('si') can define the expiry time instead of the token
                if (string.IsNullOrEmpty(sas.Identifier))
                    throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format, the 'se' parameter is missing");
            }
            else if (sas.ExpiresOn < now - SasTokenClockSkew)
            {
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} expired at {sas.ExpiresOn:O}");
            }

            if (sas.StartsOn != default && sas.StartsOn > now + SasTokenClockSkew)
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't valid before {sas.StartsOn:O}");

            if (sas.KeyExpiresOn != default && sas.KeyExpiresOn < now - SasTokenClockSkew)
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} is a user delegation SAS whose key expired at {sas.KeyExpiresOn:O}");

            return sas;
        }

        private static string GetUnrecognizedSasTokenMessage(string sasToken)
        {
            if (sasToken.StartsWith("https://", StringComparison.OrdinalIgnoreCase) || sasToken.StartsWith("http://", StringComparison.OrdinalIgnoreCase))
                return $"{nameof(AzureSettings.SasToken)} isn't in the correct format, it's a SAS URL. Use only its query part, after the '?'";

            if (sasToken.Contains("SharedAccessSignature=", StringComparison.OrdinalIgnoreCase))
                return $"{nameof(AzureSettings.SasToken)} isn't in the correct format, it's a connection string. Use only its SharedAccessSignature value";

            if (sasToken[0] is '"' or '\'')
                return $"{nameof(AzureSettings.SasToken)} isn't in the correct format, it's wrapped in quotes. Remove them";

            return $"{nameof(AzureSettings.SasToken)} isn't in the correct format, the 'sv' parameter is missing";
        }

        public void Report(long value)
        {
            _progress?.UploadProgress.ChangeState(UploadState.Uploading);
            _progress?.UploadProgress.SetUploaded(value);
            _progress?.OnUploadProgress?.Invoke();
        }

        public void Dispose()
        {
        }

        private static void FillResult(RavenStorageClient.ListBlobResult result, Page<BlobHierarchyItem> page, bool listFolders)
        {
            result.List = page.Values
                .Where(x => listFolders || x.IsBlob)
                .Select(x => listFolders ? RestorePointsBase.GetDirectoryName(x.IsPrefix ? x.Prefix : x.Blob.Name) : x.Blob.Name)
                .Distinct()
                .Select(x => new RavenStorageClient.BlobProperties { Name = x })
                .ToList();

            if (string.IsNullOrWhiteSpace(page.ContinuationToken) == false)
                result.ContinuationToken = page.ContinuationToken;
        }

        private static void VerifySasToken(string sasToken)
        {
            try
            {
                var splitted = sasToken.Split('&');
                if (splitted.Length == 0)
                    throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format");

                foreach (var keyValueString in splitted)
                {
                    var keyValue = keyValueString.Split('=');
                    if (string.IsNullOrEmpty(keyValue[0]) || string.IsNullOrEmpty(keyValue[1]))
                        throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format");
                }
            }
            catch (Exception e)
            {
                throw new ArgumentException($"{nameof(AzureSettings.SasToken)} isn't in the correct format", e);
            }
        }

        public static IRavenAzureClient Create(AzureSettings settings, BackupConfiguration configuration, Progress progress = null, CancellationToken cancellationToken = default)
        {
            if (configuration.AzureLegacy)
                return new LegacyRavenAzureClient(settings, progress, cancellationToken: cancellationToken);

            return new RavenAzureClient(settings, configuration, progress, cancellationToken);
    }

        public IMultiPartUploader GetUploader(string key, Dictionary<string, string> metadata)
        {
            return new AzureMultiPartUploader(_client, key, metadata, _progress, _cancellationToken);
        }
    }
}
