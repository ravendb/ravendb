using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.Aws;
using Sparrow;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class AwsS3MultiPartUploader : IMultiPartUploader
{
    private readonly AmazonS3Client _client;
    private readonly string _bucketName;
    private readonly Progress _progress;
    private readonly string _key;
    private readonly Dictionary<string, string> _metadata;
    private readonly CancellationToken _cancellationToken;

    private string _uploadId;
    private int _partNumber;
    private List<PartETag> _partEtags;

    public AwsS3MultiPartUploader(AmazonS3Client client, string bucketName, Progress progress, string key, Dictionary<string, string> metadata, CancellationToken cancellationToken)
    {
        _client = client;
        _bucketName = bucketName;
        _progress = progress;
        _key = key;
        _metadata = metadata;
        _cancellationToken = cancellationToken;
    }

    public void Initialize()
    {
        AsyncHelpers.RunSync(InitializeAsync);
    }

    public async Task InitializeAsync()
    {
        var multipartRequest = new InitiateMultipartUploadRequest
        {
            Key = _key,
            BucketName = _bucketName
        };

        RavenAwsS3Client.FillMetadata(multipartRequest.Metadata, _metadata);

        var initiateResponse = await _client.InitiateMultipartUploadAsync(multipartRequest, _cancellationToken);
        _uploadId = initiateResponse.UploadId;
        _partNumber = 1;
        _partEtags = new List<PartETag>();
    }

    public void UploadPart(Stream stream, long size)
    {
        AsyncHelpers.RunSync(() => UploadPartAsync(stream, size));
    }

    public async Task UploadPartAsync(Stream stream, long size)
    {
        if (_uploadId == null)
            throw new InvalidOperationException($"You must call Initialize before uploading a part");

        var uploadResponse = await _client
            .UploadPartAsync(new UploadPartRequest
            {
                Key = _key,
                BucketName = _bucketName,
                InputStream = stream,
                PartNumber = _partNumber++,
                PartSize = size,
                UploadId = _uploadId,
                StreamTransferProgress = (_, args) =>
                {
                    _progress?.UploadProgress.ChangeState(UploadState.Uploading);
                    _progress?.UploadProgress.UpdateUploaded(args.IncrementTransferred);
                    _progress?.OnUploadProgress?.Invoke();
                }
            }, _cancellationToken);

        _partEtags.Add(new PartETag(uploadResponse.PartNumber, uploadResponse.ETag));
    }

    public void CompleteUpload()
    {
        AsyncHelpers.RunSync(CompleteUploadAsync);
    }

    public async Task CompleteUploadAsync()
    {
        await _client.CompleteMultipartUploadAsync(
            new CompleteMultipartUploadRequest
            {
                UploadId = _uploadId,
                BucketName = _bucketName,
                Key = _key,
                PartETags = _partEtags
            },
            _cancellationToken);
    }
}
