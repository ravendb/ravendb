using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Extensions;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Sparrow;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public abstract class DirectUploadStream<T> : Stream where T : IDirectUploader
{
    private readonly IMultiPartUploader _multiPartUploader;
    private readonly CloudUploadStatus _cloudUploadStatus;
    private readonly Action<string> _onProgress;
    private readonly IDisposable _backupStatusIDisposable;

    private long _position;
    private MemoryStream _writeStream = new();
    private MemoryStream _uploadStream = new();
    private Task _uploadTask;
    private bool _disposed;
    private bool _abortUpload;

    protected T Client { get; }

    protected abstract long MinOnePartUploadSizeInBytes { get; }

    protected DirectUploadStream(Parameters parameters)
    {
        _cloudUploadStatus = parameters.CloudUploadStatus;
        _cloudUploadStatus.Skipped = false;
        _backupStatusIDisposable = _cloudUploadStatus.UpdateStats(parameters.IsFullBackup);
        _cloudUploadStatus.UploadProgress.ChangeState(UploadState.PendingUpload);
        _onProgress = parameters.OnProgress;

        var progress = Progress.Get(_cloudUploadStatus.UploadProgress, parameters.OnProgress);
        Client = parameters.ClientFactory.Invoke(progress);
        _multiPartUploader = Client.GetUploader(parameters.Key, parameters.Metadata);
        _multiPartUploader.Initialize();

        parameters.RegisterOnBackupException?.Invoke(() => _abortUpload = true);
    }

    public override void Flush()
    {
        // nothing to do here
    }

    public override int Read(byte[] buffer, int offset, int count)
    {
        throw new NotSupportedException();
    }

    public override long Seek(long offset, SeekOrigin origin)
    {
        throw new NotSupportedException();
    }

    public override void SetLength(long value)
    {
        throw new NotSupportedException();
    }

    public override void Write(byte[] buffer, int offset, int count)
    {
        _position += count;
        _writeStream.Write(buffer, offset, count);
        _cloudUploadStatus.UploadProgress.SetTotal(_position);

        if (_writeStream.Position <= MinOnePartUploadSizeInBytes)
            return;

        if (_uploadTask is { IsCompletedSuccessfully: false })
        {
            _onProgress.Invoke("Waiting for previous upload task to finish");
            AsyncHelpers.RunSync(() => _uploadTask);
        }

        StartUploadTask();
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        _position += count;
        await _writeStream.WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken);
        _cloudUploadStatus.UploadProgress.SetTotal(_position);

        if (_writeStream.Position <= MinOnePartUploadSizeInBytes)
            return;

        if (_uploadTask is { IsCompletedSuccessfully: false })
        {
            _onProgress.Invoke("Waiting for previous upload task to finish");
            await _uploadTask;
        }

        StartUploadTask();
    }

    private void StartUploadTask()
    {
        (_writeStream, _uploadStream) = (_uploadStream, _writeStream);
        _writeStream.SetLength(0);
        _uploadStream.Position = 0;
        _uploadTask = _multiPartUploader.UploadPartAsync(_uploadStream);
    }

    // Dispose and DisposeAsync are mirror images and must be kept in sync - neither can delegate to the other: the document
    // backup path disposes us from inside its pump, where a sync wait on _uploadTask would deadlock on it (RavenDB-26912).
    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        _disposed = true;

        using (Client)
        using (_backupStatusIDisposable)
        {
            try
            {
                using (_uploadStream)
                using (_writeStream)
                {
                    if (_abortUpload)
                    {
                        AbortUpload();
                        return;
                    }

                    if (_uploadTask is { IsCompletedSuccessfully: false })
                    {
                        _onProgress.Invoke("Waiting for previous upload task to finish");
                        AsyncHelpers.RunSync(() => _uploadTask);
                    }

                    var toUpload = _writeStream.Position;
                    if (toUpload > 0)
                    {
                        _writeStream.Position = 0;
                        _multiPartUploader.UploadPart(_writeStream);
                    }
                }

                _multiPartUploader.CompleteUpload();
            }
            catch
            {
                AbortUpload();
                throw;
            }

            _cloudUploadStatus.UploadProgress.SetUploaded(_position);
            _cloudUploadStatus.UploadProgress.SetTotal(_position);
            _cloudUploadStatus.UploadProgress.ChangeState(UploadState.Done);

            _onProgress.Invoke($"Total uploaded: {new Size(_position, SizeUnit.Bytes)}");

            OnCompleteUpload();
        }
    }

    public override async ValueTask DisposeAsync()
    {
        if (_disposed)
            return;

        _disposed = true;

        GC.SuppressFinalize(this);

        using (Client)
        using (_backupStatusIDisposable)
        {
            try
            {
                using (_uploadStream)
                using (_writeStream)
                {
                    if (_abortUpload)
                    {
                        await AbortUploadAsync().ConfigureAwait(false);
                        return;
                    }

                    if (_uploadTask is { IsCompletedSuccessfully: false })
                    {
                        _onProgress.Invoke("Waiting for previous upload task to finish");
                        await _uploadTask.ConfigureAwait(false);
                    }

                    var toUpload = _writeStream.Position;
                    if (toUpload > 0)
                    {
                        _writeStream.Position = 0;
                        await _multiPartUploader.UploadPartAsync(_writeStream).ConfigureAwait(false);
                    }
                }

                await _multiPartUploader.CompleteUploadAsync().ConfigureAwait(false);
            }
            catch
            {
                await AbortUploadAsync().ConfigureAwait(false);
                throw;
            }

            _cloudUploadStatus.UploadProgress.SetUploaded(_position);
            _cloudUploadStatus.UploadProgress.SetTotal(_position);
            _cloudUploadStatus.UploadProgress.ChangeState(UploadState.Done);

            _onProgress.Invoke($"Total uploaded: {new Size(_position, SizeUnit.Bytes)}");

            OnCompleteUpload();
        }
    }

    private void AbortUpload()
    {
        PrepareForAbort();

        try
        {
            _multiPartUploader.Abort();
        }
        catch (Exception e)
        {
            ReportAbortFailure(e);
        }
    }

    private async Task AbortUploadAsync()
    {
        PrepareForAbort();

        try
        {
            await _multiPartUploader.AbortAsync().ConfigureAwait(false);
        }
        catch (Exception e)
        {
            ReportAbortFailure(e);
        }
    }

    private void PrepareForAbort()
    {
        _cloudUploadStatus.UploadProgress.ChangeState(UploadState.Aborted);
        _uploadTask?.IgnoreUnobservedExceptions();
    }

    private void ReportAbortFailure(Exception e)
    {
        _onProgress.Invoke($"Failed to abort the multipart upload, incomplete parts may remain at the destination: {e}");
    }

    protected abstract void OnCompleteUpload();

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => _position;
        set
        {
            throw new NotSupportedException();
        }
    }

    public class Parameters
    {
        public Func<Progress, T> ClientFactory { get; set; }

        public string Key { get; set; }

        public Dictionary<string, string> Metadata { get; set; }

        public bool IsFullBackup { get; set; }

        public RetentionPolicyBaseParameters RetentionPolicyParameters { get; set; }

        public CloudUploadStatus CloudUploadStatus { get; set; }

        public Action<Action> RegisterOnBackupException { get; set; }

        public Action<string> OnProgress { get; set; }
    }
}
