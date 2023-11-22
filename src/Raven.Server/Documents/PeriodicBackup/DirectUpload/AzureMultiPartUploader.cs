using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Blobs.Specialized;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public class AzureMultiPartUploader : IMultiPartUploader
{
    private readonly BlockBlobClient _blockBlobClient;
    private readonly Dictionary<string, string> _metadata;
    private readonly Progress _progress;
    private readonly CancellationToken _cancellationToken;

    private List<string> _base64BlockIds = new();
    private int _partNumber = 1;

    public AzureMultiPartUploader(BlobContainerClient client, string key, Dictionary<string, string> metadata, Progress progress, CancellationToken cancellationToken)
    {
        _blockBlobClient = client.GetBlockBlobClient(key);
        _metadata = metadata;
        _progress = progress;
        _cancellationToken = cancellationToken;
    }

    public void Initialize()
    {
    }

    public Task InitializeAsync()
    { 
        return Task.CompletedTask;
    }

    public void UploadPart(Stream stream)
    {
        AsyncHelpers.RunSync(() => UploadPartAsync(stream));
    }

    public async Task UploadPartAsync(Stream stream)
    {
        var blockID = Convert.ToBase64String(BitConverter.GetBytes(_partNumber++));
        _base64BlockIds.Add(blockID);

        var uploadedSoFar = _progress.UploadProgress.UploadedInBytes;
        await _blockBlobClient.StageBlockAsync(blockID, stream, new BlockBlobStageBlockOptions
        {
            ProgressHandler = new Progress<long>(value =>
            {
                if (_progress?.UploadProgress == null)
                    return;

                _progress.UploadProgress.ChangeState(UploadState.Uploading);
                _progress.UploadProgress.SetUploaded(uploadedSoFar + value);
                _progress.OnUploadProgress?.Invoke();
            })
        }, _cancellationToken);
    }

    public void CompleteUpload()
    {
        AsyncHelpers.RunSync(CompleteUploadAsync);
    }

    public async Task CompleteUploadAsync()
    {
        await _blockBlobClient.CommitBlockListAsync(_base64BlockIds, new CommitBlockListOptions
        {
            Metadata = _metadata
        }, _cancellationToken);
    }
}
