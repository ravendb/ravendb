using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Api.Gax;
using Google.Apis.Auth.OAuth2;
using Google.Apis.Storage.v1.Data;
using Google.Apis.Upload;
using Google.Cloud.Storage.V1;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Operations.Backups;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace Raven.Server.Documents.PeriodicBackup.GoogleCloud
{
    public class RavenGoogleCloudClient : IDisposable
    {
        private readonly StorageClient _client;
        private readonly string _projectId;
        protected readonly CancellationToken CancellationToken;
        private readonly string _bucketName;
        private readonly Progress _progress;

        private const string ProjectIdPropertyName = "project_id";

        public RavenGoogleCloudClient(GoogleCloudSettings settings, Progress progress = null, CancellationToken? cancellationToken = null)
        {
            if (string.IsNullOrWhiteSpace(settings.BucketName))
                throw new ArgumentException("Google cloud bucket name cannot be null or empty");

            if (string.IsNullOrWhiteSpace(settings.GoogleCredentialsJson))
                throw new ArgumentException("Google Credentials Json cannot be null or empty");
            try
            {
                _client = StorageClient.Create(GoogleCredential.FromJson(settings.GoogleCredentialsJson));
            }
            catch (Exception e)
            {
                throw new ArgumentException("Wrong format for Google Credentials.", e);
            }

            var credentialJsonType = JObject.Parse(settings.GoogleCredentialsJson);

            if (credentialJsonType.TryGetValue(ProjectIdPropertyName, StringComparison.OrdinalIgnoreCase, out var value))
            {
                _projectId = value.Value<string>();
            }

            _bucketName = settings.BucketName;
            CancellationToken = cancellationToken ?? CancellationToken.None;

            _progress = progress;
        }

        public Object UploadObject(string fileName, Stream stream, Dictionary<string, string> metadata = null)
        {
            return _client.UploadObject(
                new Object { Bucket = _bucketName, Name = fileName, ContentType = "application/octet-stream", Metadata = metadata }, stream,
                progress: new Progress<IUploadProgress>(p =>
                {
                    if (_progress == null)
                        return;

                    switch (p.Status)
                    {
                        case UploadStatus.Starting:
                        case UploadStatus.NotStarted:
                            _progress.UploadProgress.ChangeState(UploadState.PendingUpload);
                            break;
                        case UploadStatus.Completed:
                            _progress.UploadProgress.ChangeState(UploadState.Done);
                            break;
                        case UploadStatus.Uploading:
                            _progress.UploadProgress.ChangeState(UploadState.Uploading);
                            break;
                    }

                    _progress.UploadProgress.UploadedInBytes = p.BytesSent;
                    _progress.OnUploadProgress();
                }));
        }

        public Task<Object> UploadObjectAsync(string fileName, Stream stream, Dictionary<string, string> metadata = null)
        {
            return _client.UploadObjectAsync(
                new Object {Bucket = _bucketName, Name = fileName, ContentType = "application/octet-stream", Metadata = metadata}, stream,
                cancellationToken: CancellationToken,
                progress: new Progress<IUploadProgress>(p =>
                {
                    if (_progress == null)
                        return;

                    switch (p.Status)
                    {
                        case UploadStatus.Starting:
                        case UploadStatus.NotStarted:
                            _progress.UploadProgress.ChangeState(UploadState.PendingUpload);
                            break;
                        case UploadStatus.Completed:
                            _progress.UploadProgress.ChangeState(UploadState.Done);
                            break;
                        case UploadStatus.Uploading:
                            _progress.UploadProgress.ChangeState(UploadState.Uploading);
                            break;
                    }

                    _progress.UploadProgress.UploadedInBytes = p.BytesSent;
                    _progress.OnUploadProgress();
                }));
        }

        public ConfiguredTaskAwaitable DownloadObjectAsync(string fileName, Stream stream)
        {
            return _client.DownloadObjectAsync(
                _bucketName,
                fileName,
                cancellationToken: CancellationToken,
                destination: stream
            ).ConfigureAwait(false);
        }

        public ConfiguredTaskAwaitable<Object> GetObjectAsync(string fileName)
        {
            return _client.GetObjectAsync(
                _bucketName,
                fileName,
                cancellationToken: CancellationToken
            ).ConfigureAwait(false);
        }

        public ConfiguredTaskAwaitable DeleteObjectAsync(string fileName)
        {
            return _client.DeleteObjectAsync(
                _bucketName,
                fileName,
                null,
                CancellationToken
            ).ConfigureAwait(false);
        }

        public PagedEnumerable<Buckets, Bucket> ListBuckets()
        {
            if (_projectId == null)
                throw new ArgumentException($"Project Id was not found in Google credentials json, " +
                                            $"please make sure that '{ProjectIdPropertyName}' is part of the credentials");

            return _client.ListBuckets(_projectId);
        }

        public Task<List<Object>> ListObjectsAsync(string prefix = null,string delimiter = null)
        {
            var option = new ListObjectsOptions
            {
                Delimiter = delimiter
            };
            
            return _client.ListObjectsAsync(_bucketName, prefix, options: delimiter == null ? null : option).ToList(CancellationToken);
        }

        public async Task TestConnection()
        {
            try
            {
                await _client.GetBucketAsync(_bucketName, cancellationToken: CancellationToken);

                if (await _client.TestBucketIamPermissionsAsync(_bucketName, new[] {"storage.objects.create"}, cancellationToken: CancellationToken) == null)
                {
                    throw new InvalidOperationException(
                        $"Can't create an object in bucket '{_bucketName}', " +
                        $"The permission 'storage.objects.create' is missing");
                }
            }
            catch (Google.GoogleApiException e)
                when (e.Error.Code == 403)
            {
                throw new InvalidOperationException($"Google credentials json does not have access to project {_projectId ?? "N/A"}", e);
            }
            catch (Google.GoogleApiException e)
                when (e.Error.Code == 404)
            {
                throw new InvalidOperationException($"Bucket {_bucketName} not found", e);
            }
        }

        public void Dispose()
        {
            _client?.Dispose();
        }
    }
}
