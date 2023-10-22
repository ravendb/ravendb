using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.Backups;
using Raven.Client.Util;
using Raven.Server.Documents.PeriodicBackup.Retention;
using Sparrow;
using Size = Sparrow.Size;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public abstract class DirectUploadStream<T> : Stream where T : IDirectUploader
{
    private readonly IMultiPartUploader _multiPartUploader;
    private readonly UploadToS3 _cloudUploadStatus;
    private readonly Action<string> _onProgress;
    private readonly IDisposable _backupStatusIDisposable;

    private long _position;
    private MemoryStream _writeStream = new();
    private MemoryStream _uploadStream = new();
    private Task _uploadTask;
    private bool _disposed;

    protected T Client { get; }

    protected abstract long MaxPartSizeInBytes { get; }

    protected DirectUploadStream(Parameters parameters)
    {
        _cloudUploadStatus = parameters.CloudUploadStatus;
        _cloudUploadStatus.Skipped = false;
        _backupStatusIDisposable = _cloudUploadStatus.UpdateStats(parameters.IsFullBackup);
        _cloudUploadStatus.UploadProgress.ChangeState(UploadState.PendingUpload);
        _onProgress = parameters.OnProgress;

        var progress = Progress.Get(_cloudUploadStatus.UploadProgress, parameters.OnProgress);
        Client = parameters.Client.Invoke(progress);
        _multiPartUploader = Client.GetUploader(parameters.Key, parameters.Metadata);
        _multiPartUploader.Initialize();
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
        _uploadTask = _multiPartUploader.UploadPartAsync(_uploadStream, toUpload);
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
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
        _uploadTask = _multiPartUploader.UploadPartAsync(_uploadStream, toUpload);
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
                    _multiPartUploader.UploadPart(_writeStream, toUpload);
                }
            }

            _multiPartUploader.CompleteUpload();

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

    public class Parameters
    {
        public Func<Progress, T> Client { get; set; }

        public string Key { get; set; }

        public Dictionary<string, string> Metadata { get; set; }

        public bool IsFullBackup { get; set; }

        public RetentionPolicyBaseParameters RetentionPolicyParameters { get; set; }

        public UploadToS3 CloudUploadStatus { get; set; }

        public Action<string> OnProgress { get; set; }
    }
}
