using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;
using Google.Apis.Upload;
using Google.Cloud.Storage.V1;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Object = Google.Apis.Storage.v1.Data.Object;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

/// <summary>
/// Google Cloud Storage has no "upload part" primitive: an object is streamed through a single resumable upload session,
/// which the SDK drives by pulling the data from a stream. This uploader bridges the push model of <see cref="DirectUploadStream{T}"/>
/// to that pull model: the parts are written into a pipe, and a background task runs the SDK upload reading from the other end of it.
/// The SDK takes care of the chunking (aligned to 256 KiB), of retrying and resuming the session and of validating the CRC32C of the object.
/// </summary>
public sealed class GoogleCloudMultiPartUploader : IMultiPartUploader
{
    private const string ContentType = "application/octet-stream";
    private const int WriteBufferSizeHint = 81920;

    private readonly StorageClient _client;
    private readonly string _bucketName;
    private readonly string _key;
    private readonly Dictionary<string, string> _metadata;
    private readonly int _chunkSize;
    private readonly Progress _progress;
    private readonly CancellationToken _cancellationToken;
    private readonly object _progressLock = new();

    private bool _aborted;
    private Pipe _pipe;
    private CancellationTokenSource _uploadCts;
    private Task _uploadTask;
    private Task _partTask;

    public GoogleCloudMultiPartUploader(StorageClient client, string bucketName, string key, Dictionary<string, string> metadata, int chunkSize,
        Progress progress, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(client);

        if (chunkSize <= 0 || chunkSize % UploadObjectOptions.MinimumChunkSize != 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSize), chunkSize,
                $"The chunk size must be a positive multiple of {UploadObjectOptions.MinimumChunkSize} bytes");

        _client = client;
        _bucketName = bucketName;
        _key = key;
        _metadata = metadata;
        _chunkSize = chunkSize;
        _progress = progress;
        _cancellationToken = cancellationToken;
    }

    public void Initialize()
    {
        // called synchronously from the DirectUploadStream constructor, so it only starts the upload and never blocks
        if (_uploadTask != null)
            throw new InvalidOperationException("The upload was already initialized");

        _uploadCts = CancellationTokenSource.CreateLinkedTokenSource(_cancellationToken);
        // up to one chunk is buffered while the SDK sends the previous one
        _pipe = new Pipe(new PipeOptions(
            pauseWriterThreshold: _chunkSize,
            resumeWriterThreshold: _chunkSize / 2,
            useSynchronizationContext: false));

        // the upload runs on the thread pool, outside of the backup's synchronization context (RavenDB-26903)
        _uploadTask = Task.Run(UploadAsync);
    }

    public Task InitializeAsync()
    {
        Initialize();
        return Task.CompletedTask;
    }

    private async Task UploadAsync()
    {
        var reader = _pipe.Reader;

        try
        {
            await using (var source = reader.AsStream(leaveOpen: true))
            {
                await _client.UploadObjectAsync(
                    new Object
                    {
                        Bucket = _bucketName,
                        Name = _key,
                        ContentType = ContentType,
                        Metadata = _metadata
                    },
                    source,
                    new UploadObjectOptions
                    {
                        ChunkSize = _chunkSize,
                        UploadValidationMode = UploadValidationMode.RejectAndThrow
                    },
                    _uploadCts.Token,
                    new UploadProgressReporter(this)).ConfigureAwait(false);
            }
        }
        catch (Exception e)
        {
            // wakes up a writer that waits for the pipe to drain
            await reader.CompleteAsync(e).ConfigureAwait(false);
            throw;
        }

        await reader.CompleteAsync().ConfigureAwait(false);
    }

    public void UploadPart(Stream stream)
    {
        AsyncHelpers.RunSync(() => UploadPartAsync(stream));
    }

    public Task UploadPartAsync(Stream stream)
    {
        // the DirectUploadStream doesn't wait for the part that is in flight when it aborts, the abort does
        return _partTask = UploadPartInternalAsync(stream);
    }

    private async Task UploadPartInternalAsync(Stream stream)
    {
        AssertInitialized();

        var token = _uploadCts.Token;

        if (_uploadTask.IsCompleted)
            await ThrowUploadEndedPrematurelyAsync().ConfigureAwait(false);

        // the data is copied into memory owned by the pipe, the caller is free to reuse the stream once we're done
        var writer = _pipe.Writer;
        while (true)
        {
            var memory = writer.GetMemory(WriteBufferSizeHint);
            var read = await stream.ReadAsync(memory, token).ConfigureAwait(false);
            if (read == 0)
                break;

            writer.Advance(read);

            var result = await writer.FlushAsync(token).ConfigureAwait(false);
            if (result.IsCompleted || result.IsCanceled)
                await ThrowUploadEndedPrematurelyAsync().ConfigureAwait(false);
        }
    }

    public void CompleteUpload()
    {
        AsyncHelpers.RunSync(CompleteUploadAsync);
    }

    public async Task CompleteUploadAsync()
    {
        AssertInitialized();

        // a clean end of the stream is the signal for the SDK to send the last chunk and create the object
        await _pipe.Writer.CompleteAsync().ConfigureAwait(false);
        await _uploadTask.ConfigureAwait(false);

        _uploadCts.Dispose();
    }

    public void Abort()
    {
        AsyncHelpers.RunSync(AbortAsync);
    }

    public async Task AbortAsync()
    {
        if (_uploadTask == null)
            return;

        lock (_progressLock)
        {
            _aborted = true;

            // the DirectUploadStream marks the upload as aborted before aborting it,
            // a progress report of the still running upload might have overridden that
            _progress?.UploadProgress?.ChangeState(UploadState.Aborted);
        }

        // the object is created only by the last request of the session, so an unfinished session leaves nothing behind
        // and it expires on its own. cancelling also releases a part upload that might still wait for the pipe to drain
        if (_uploadTask.IsCompleted == false || _partTask is { IsCompleted: false })
            await _uploadCts.CancelAsync().ConfigureAwait(false);

        // the reader must never see a clean end of the stream, otherwise the SDK would create a truncated object
        await _pipe.Writer.CompleteAsync(new OperationCanceledException("The upload to Google Cloud was aborted")).ConfigureAwait(false);

        // expected to fail, the upload was aborted or it has already failed
        await IgnoreFailureAsync(_partTask).ConfigureAwait(false);
        await IgnoreFailureAsync(_uploadTask).ConfigureAwait(false);

        _uploadCts.Dispose();
    }

    private static async Task IgnoreFailureAsync(Task task)
    {
        if (task == null)
            return;

        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            // ignored
        }
    }

    private async Task ThrowUploadEndedPrematurelyAsync()
    {
        // the reader side is done although we didn't complete the writer, so the upload must have failed - surface its error
        await _uploadTask.ConfigureAwait(false);

        throw new InvalidOperationException($"The upload of '{_key}' to Google Cloud bucket '{_bucketName}' has ended before all of the data was written");
    }

    private void AssertInitialized()
    {
        if (_uploadTask == null)
            throw new InvalidOperationException("You must call Initialize before uploading");
    }

    private void ReportProgress(IUploadProgress value)
    {
        if (_progress?.UploadProgress == null)
            return;

        if (value.Status != UploadStatus.Uploading && value.Status != UploadStatus.Completed)
            return;

        lock (_progressLock)
        {
            if (_aborted)
                return;

            // a single upload per object, so the bytes sent are the total uploaded so far.
            // the final state (Done / Aborted) is set by the DirectUploadStream
            _progress.UploadProgress.ChangeState(UploadState.Uploading);
            _progress.UploadProgress.SetUploaded(value.BytesSent);
        }

        _progress.OnUploadProgress?.Invoke();
    }

    // System.Progress<T> would post the callbacks to the synchronization context that is current when it's created
    private sealed class UploadProgressReporter : IProgress<IUploadProgress>
    {
        private readonly GoogleCloudMultiPartUploader _uploader;

        public UploadProgressReporter(GoogleCloudMultiPartUploader uploader)
        {
            _uploader = uploader;
        }

        public void Report(IUploadProgress value) => _uploader.ReportProgress(value);
    }
}
