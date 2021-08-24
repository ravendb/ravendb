using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.Restore;
using Sparrow;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup.Aws
{
    public class RavenAwsS3Client : IDisposable
    {
        private readonly Progress _progress;
        private readonly CancellationToken _cancellationToken;
        internal Size MaxUploadPutObject = new Size(256, SizeUnit.Megabytes);
        internal Size MinOnePartUploadSizeLimit = new Size(100, SizeUnit.Megabytes);

        private static readonly Size TotalBlocksSizeLimit = new Size(5, SizeUnit.Terabytes);

        private AmazonS3Client _client;
        private readonly string _bucketName;

        public readonly string RemoteFolderName;

        public readonly string Region;

        public RavenAwsS3Client(S3Settings s3Settings, Progress progress = null, CancellationToken cancellationToken = default)
        {
            if (s3Settings == null)
                throw new ArgumentNullException(nameof(s3Settings));

            if (string.IsNullOrWhiteSpace(s3Settings.AwsAccessKey))
                throw new ArgumentException("AWS Access Key cannot be null or empty");

            if (string.IsNullOrWhiteSpace(s3Settings.AwsSecretKey))
                throw new ArgumentException("AWS Secret Key cannot be null or empty");

            if (string.IsNullOrWhiteSpace(s3Settings.BucketName))
                throw new ArgumentException("AWS Bucket Name cannot be null or empty");

            var config = new AmazonS3Config();
            if (string.IsNullOrWhiteSpace(s3Settings.CustomServerUrl))
            {
                if (string.IsNullOrWhiteSpace(s3Settings.AwsRegionName))
                    throw new ArgumentException("AWS Region Name cannot be null or empty");

                config.RegionEndpoint = RegionEndpoint.GetBySystemName(s3Settings.AwsRegionName);
            }
            else
            {
                config.UseHttp = true;
                config.ServiceURL = s3Settings.CustomServerUrl;
            }

            AWSCredentials credentials;
            if (string.IsNullOrWhiteSpace(s3Settings.AwsSessionToken))
                credentials = new BasicAWSCredentials(s3Settings.AwsAccessKey, s3Settings.AwsSecretKey);
            else
                credentials = new SessionAWSCredentials(s3Settings.AwsAccessKey, s3Settings.AwsSecretKey, s3Settings.AwsSessionToken);

            _client = new AmazonS3Client(credentials, config);
            _bucketName = s3Settings.BucketName;
            RemoteFolderName = s3Settings.RemoteFolderName;
            Region = s3Settings.AwsRegionName == null ? string.Empty : s3Settings.AwsRegionName.ToLower();
            _progress = progress;
            _cancellationToken = cancellationToken;
        }

        public void PutObject(string key, Stream stream, Dictionary<string, string> metadata)
        {
            AsyncHelpers.RunSync(() => PutObjectAsync(key, stream, metadata));
        }

        public async Task PutObjectAsync(string key, Stream stream, Dictionary<string, string> metadata)
        {
            //TestConnection();

            var streamSize = new Size(stream.Length, SizeUnit.Bytes);
            if (streamSize > TotalBlocksSizeLimit)
                throw new InvalidOperationException($@"Can't upload more than 5TB to AWS S3, current upload size: {streamSize}");

            var streamLength = streamSize.GetValue(SizeUnit.Bytes);
            try
            {
                _progress?.UploadProgress.SetTotal(streamLength);

                if (streamSize > MaxUploadPutObject)
                {
                    _progress?.UploadProgress.ChangeType(UploadType.Chunked);

                    var multipartRequest = new InitiateMultipartUploadRequest { Key = key, BucketName = _bucketName };

                    FillMetadata(multipartRequest.Metadata, metadata);

                    var initiateResponse = await _client.InitiateMultipartUploadAsync(multipartRequest, _cancellationToken);
                    var partNumber = 1;
                    var partEtags = new List<PartETag>();

                    while (stream.Position < streamLength)
                    {
                        var leftToUpload = streamLength - stream.Position;
                        var toUpload = Math.Min(MinOnePartUploadSizeLimit.GetValue(SizeUnit.Bytes), leftToUpload);

                        var uploadResponse = await _client
                            .UploadPartAsync(new UploadPartRequest
                            {
                                Key = key,
                                BucketName = _bucketName,
                                InputStream = stream,
                                PartNumber = partNumber++,
                                PartSize = toUpload,
                                UploadId = initiateResponse.UploadId,
                                StreamTransferProgress = (_, args) => _progress?.UploadProgress.UpdateUploaded(args.IncrementTransferred)
                            }, _cancellationToken);

                        partEtags.Add(new PartETag(uploadResponse.PartNumber, uploadResponse.ETag));
                    }

                    await _client.CompleteMultipartUploadAsync(
                        new CompleteMultipartUploadRequest { UploadId = initiateResponse.UploadId, BucketName = _bucketName, Key = key, PartETags = partEtags },
                        _cancellationToken);

                    return;
                }

                var request = new PutObjectRequest
                {
                    Key = key,
                    BucketName = _bucketName,
                    InputStream = stream,
                    StreamTransferProgress = (_, args) => _progress?.UploadProgress.UpdateUploaded(args.IncrementTransferred)
                };

                FillMetadata(request.Metadata, metadata);

                await _client.PutObjectAsync(request, _cancellationToken);
            }
            catch (AmazonS3Exception e)
            {
                await MaybeHandleExceptionAsync(e);

                throw;
            }
            finally
            {
                _progress?.UploadProgress.ChangeState(UploadState.Done);
            }
        }

        public async Task<List<S3FileInfoDetails>> ListAllObjectsAsync(string prefix, string delimiter, bool listFolders)
        {
            var allObjects = new List<S3FileInfoDetails>();

            string continuationToken = null;

            while (true)
            {
                var objects = await ListObjectsAsync(prefix, delimiter, listFolders, continuationToken: continuationToken);
                allObjects.AddRange(objects.FileInfoDetails);

                continuationToken = objects.ContinuationToken;
                if (continuationToken == null)
                    break;
            }

            return allObjects;
        }

        public async Task<ListObjectsResult> ListObjectsAsync(string prefix, string delimiter, bool listFolders, bool includeFolders = false, int? take = null, string continuationToken = null, string startAfter = null)
        {
            try
            {
                var response = await _client.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = _bucketName,
                    ContinuationToken = continuationToken,
                    Delimiter = delimiter,
                    Prefix = prefix,
                    StartAfter = startAfter
                }, _cancellationToken);

                var result = new ListObjectsResult
                {
                    ContinuationToken = response.NextContinuationToken,
                };

                if (listFolders)
                {
                    result.FileInfoDetails = response
                        .CommonPrefixes
                        .Select(x => new S3FileInfoDetails { FullPath = x })
                        .ToList();
                }
                else
                {
                    result.FileInfoDetails = response
                        .S3Objects
                        .Select(x => new S3FileInfoDetails { FullPath = x.Key, LastModified = x.LastModified })
                        .ToList();
                }

                return result;
            }
            catch (AmazonS3Exception e)
            {
                await MaybeHandleExceptionAsync(e);

                throw;
            }
        }

        public ListObjectsResult ListObjects(string prefix, string delimiter, bool listFolders, bool includeFolders = false, int? take = null, string continuationToken = null, string startAfter = null)
        {
            return AsyncHelpers.RunSync(() => ListObjectsAsync(prefix, delimiter, listFolders, includeFolders, take, continuationToken, startAfter));
        }

        public async Task<RavenStorageClient.Blob> GetObjectAsync(string key)
        {
            try
            {
                var response = await _client.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = _bucketName,
                    Key = key
                }, _cancellationToken);

                return new RavenStorageClient.Blob(response.ResponseStream, ConvertMetadata(response.Metadata), response);
            }
            catch (AmazonS3Exception e)
            {
                await MaybeHandleExceptionAsync(e);

                throw;
            }
        }

        public void DeleteObject(string key)
        {
            AsyncHelpers.RunSync(() => DeleteObjectAsync(key));
        }

        public async Task DeleteObjectAsync(string key)
        {
            try
            {
                await _client.DeleteObjectAsync(new DeleteObjectRequest
                {
                    BucketName = _bucketName,
                    Key = key
                }, _cancellationToken);
            }
            catch (AmazonS3Exception e)
            {
                await MaybeHandleExceptionAsync(e);

                throw;
            }
        }


        public void DeleteMultipleObjects(List<string> objects)
        {
            AsyncHelpers.RunSync(() => DeleteMultipleObjectsAsync(objects));
        }

        private async Task DeleteMultipleObjectsAsync(List<string> objects)
        {
            try
            {
                await _client.DeleteObjectsAsync(new DeleteObjectsRequest
                {
                    BucketName = _bucketName,
                    Objects = objects.Select(x => new KeyVersion
                    {
                        Key = x
                    }).ToList()
                }, _cancellationToken);
            }
            catch (AmazonS3Exception e)
            {
                await MaybeHandleExceptionAsync(e);

                throw;
            }
        }

        public void TestConnection()
        {
            AsyncHelpers.RunSync(TestConnectionAsync);
        }

        private async Task TestConnectionAsync()
        {
            await AssertBucketLocationAsync();

            await AssertBucketPermissionsAsync();
        }

        public void Dispose()
        {
            _client?.Dispose();
            _client = null;
        }

        private async Task AssertBucketPermissionsAsync()
        {
            var aclResponse = await _client.GetACLAsync(_bucketName, _cancellationToken);
            var permissions = aclResponse
                .AccessControlList
                .Grants
                .Select(x => x.Permission.Value)
                .ToHashSet();

            if (permissions.Contains("FULL_CONTROL") == false && permissions.Contains("WRITE"))
            {
                throw new InvalidOperationException(
                    $"Can't create an object in bucket '{_bucketName}', " +
                    $"when permission is set to '{string.Join(", ", permissions)}'");
            }
        }

        private async Task AssertBucketLocationAsync()
        {
            var bucketLocationResponse = await _client.GetBucketLocationAsync(_bucketName, _cancellationToken);
            var bucketLocation = bucketLocationResponse.Location.Value;
            if (string.IsNullOrEmpty(bucketLocation))
                bucketLocation = "us-east-1";

            if (bucketLocation.Equals(Region, StringComparison.OrdinalIgnoreCase) == false)
            {
                throw new InvalidOperationException($"AWS location is set to '{Region}', but the bucket named: '{_bucketName}' is located in: {bucketLocation}");
            }
        }

        private static void FillMetadata(MetadataCollection collection, IDictionary<string, string> metadata)
        {
            if (metadata == null)
                return;

            foreach (var kvp in metadata)
                collection[kvp.Key] = kvp.Value;
        }

        private static IDictionary<string, string> ConvertMetadata(MetadataCollection collection)
        {
            var metadata = new Dictionary<string, string>();
            if (collection == null)
                return metadata;

            foreach (var key in collection.Keys)
                metadata[key] = collection[key];

            return metadata;
        }

        private async Task MaybeHandleExceptionAsync(AmazonS3Exception exception)
        {
            switch (exception.StatusCode)
            {
                case HttpStatusCode.Moved:
                    await AssertBucketLocationAsync();
                    break;
                case HttpStatusCode.Forbidden:
                    await AssertBucketPermissionsAsync();
                    break;
            }
        }
    }
}
