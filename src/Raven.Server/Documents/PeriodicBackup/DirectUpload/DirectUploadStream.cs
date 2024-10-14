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

        parameters.OnBackupException += () => _abortUpload = true;
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

        if (_uploadTask != null && (_uploadTask.IsCompleted == false || _uploadTask.IsCompletedSuccessfully == false))
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

        if (_uploadTask != null && (_uploadTask.IsCompleted == false || _uploadTask.IsCompletedSuccessfully == false))
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

    protected override void Dispose(bool disposing)
    {
        if (_disposed)
            return;

        _disposed = true;

        if (_abortUpload)
        {
            using (Client)
            using (_backupStatusIDisposable)
            using (_uploadStream)
            using (_writeStream)
                return;
        }

        using (Client)
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
                    _multiPartUploader.UploadPart(_writeStream);
                }
            }

            _multiPartUploader.CompleteUpload();

            _cloudUploadStatus.UploadProgress.SetUploaded(_position);
            _cloudUploadStatus.UploadProgress.SetTotal(_position);
            _cloudUploadStatus.UploadProgress.ChangeState(UploadState.Done);

            _onProgress.Invoke($"Total uploaded: {new Size(_position, SizeUnit.Bytes)}");

            OnCompleteUpload();
        }
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

        public Action OnBackupException { get; set; }

        public Action<string> OnProgress { get; set; }
    }
}
