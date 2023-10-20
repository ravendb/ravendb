using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Sparrow;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public abstract class DirectUploadStream : Stream
{
    private readonly UploadToS3 _cloudUploadStatus;
    private readonly Action<string> _onProgress;

    protected abstract IMultiPartUploader MultiPartUploader { get; }

    protected abstract long MaxPartSizeInBytes { get; }

    protected Progress Progress { get; }

    private long _position;
    private bool _initialized;
    private MemoryStream _writeStream = new();
    private MemoryStream _uploadStream = new();
    private Task _uploadTask;
    private bool _disposed;
    private readonly IDisposable _backupStatusIDisposable;

    protected DirectUploadStream(bool isFullBackup, UploadToS3 cloudUploadStatus, Action<string> onProgress)
    {
        _cloudUploadStatus = cloudUploadStatus;
        _onProgress = onProgress;

        _cloudUploadStatus.Skipped = false;
        _backupStatusIDisposable = _cloudUploadStatus.UpdateStats(isFullBackup);

        _cloudUploadStatus.UploadProgress.ChangeState(UploadState.PendingUpload);

        Progress = Progress.Get(_cloudUploadStatus.UploadProgress, onProgress);
    }

    public override void Flush()
    {
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
        if (_initialized == false)
        {
            _initialized = true;
            MultiPartUploader.Initialize();
        }

        _position += count;
        _writeStream.Write(buffer, offset, count);
        _cloudUploadStatus.UploadProgress.SetTotal(_position);

        var toUpload = _writeStream.Position;
        if (toUpload <= MaxPartSizeInBytes)
            return;

        if (_uploadTask != null && (_uploadTask.IsCompleted == false || _uploadTask.IsCompletedSuccessfully == false))
        {
            _onProgress.Invoke("Waiting for previous upload task to finish");
            AsyncHelpers.RunSync(() => _uploadTask);
        }

        (_writeStream, _uploadStream) = (_uploadStream, _writeStream);
        _writeStream.Position = _uploadStream.Position = 0;
        _uploadTask = MultiPartUploader.UploadPartAsync(_uploadStream, toUpload);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_initialized == false)
        {
            _initialized = true;
            await MultiPartUploader.InitializeAsync();
        }

        _position += count;

        await _writeStream.WriteAsync(new ReadOnlyMemory<byte>(buffer, offset, count), cancellationToken);
        _cloudUploadStatus.UploadProgress.SetTotal(_position);

        var toUpload = _writeStream.Position;
        if (toUpload <= MaxPartSizeInBytes)
            return;

        if (_uploadTask != null && (_uploadTask.IsCompleted == false || _uploadTask.IsCompletedSuccessfully == false))
        {
            _onProgress.Invoke("Waiting for previous upload task to finish");
            await _uploadTask;
        }

        (_writeStream, _uploadStream) = (_uploadStream, _writeStream);
        _writeStream.Position = _uploadStream.Position = 0;
        _uploadTask = MultiPartUploader.UploadPartAsync(_uploadStream, toUpload);
    }

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        _disposed = true;

        using (_backupStatusIDisposable)
        {
            using (_uploadStream)
            using (_writeStream)
            {
                if (_uploadTask != null && (_uploadTask.IsCompleted == false || _uploadTask.IsCompletedSuccessfully == false))
                {
                    _onProgress.Invoke("Waiting for previous upload task to finish");
                    AsyncHelpers.RunSync(() => _uploadTask);
                }

                var toUpload = _writeStream.Position;
                if (toUpload > 0)
                {
                    _writeStream.Position = 0;
                    MultiPartUploader.UploadPart(_writeStream, toUpload);
                }
            }

            MultiPartUploader.CompleteUpload();

            _cloudUploadStatus.UploadProgress.SetUploaded(_position);
            _cloudUploadStatus.UploadProgress.SetTotal(_position);
            _cloudUploadStatus.UploadProgress.ChangeState(UploadState.Done);

            _onProgress.Invoke($"Total uploaded: {new Size(_position, SizeUnit.Bytes)}");
        }
    }

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
}
