using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Util;

namespace Raven.Server.Documents.PeriodicBackup.DirectUpload;

public abstract class DirectUploadStream : Stream
{
    protected abstract IMultiPartUploader MultiPartUploader { get; }
    protected abstract long MaxPartSizeInBytes { get; }

    private long _position;
    private bool _initialized;
    private MemoryStream _writeStream = new();
    private MemoryStream _uploadStream = new();
    private Task _uploadTask;

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

        if (_writeStream.Position > MaxPartSizeInBytes)
        {
            if (_uploadTask != null && (_uploadTask.IsCompleted == false || _uploadTask.IsCompletedSuccessfully == false))
                AsyncHelpers.RunSync(() => _uploadTask);

            (_writeStream, _uploadStream) = (_uploadStream, _writeStream);

            _writeStream.Position = _uploadStream.Position = 0;
            _uploadTask = MultiPartUploader.UploadPartAsync(_uploadStream, _uploadStream.Length);
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        if (_initialized == false)
        {
            _initialized = true;
            await MultiPartUploader.InitializeAsync();
        }

        _position += count;

        await _writeStream.WriteAsync(buffer, offset, count, cancellationToken);

        if (_writeStream.Position > MaxPartSizeInBytes)
        {
            if (_uploadTask != null)
            {
                if (_uploadTask.IsCompleted == false || _uploadTask.IsCompletedSuccessfully == false)
                    await _uploadTask;

                //TODO
                //_backupResult.AddInfo(message);
                //_onProgress.Invoke(_backupResult.Progress);
            }

            (_writeStream, _uploadStream) = (_uploadStream, _writeStream);

            _writeStream.Position = _uploadStream.Position = 0;
            _uploadTask = MultiPartUploader.UploadPartAsync(_uploadStream, _uploadStream.Length);
        }
    }

    protected override void Dispose(bool disposing)
    {
        using (_uploadStream)
        using (_writeStream)
        {
            if (_uploadTask != null && _uploadTask.IsCompleted == false)
                AsyncHelpers.RunSync(() => _uploadTask);

            if (_writeStream.Position > 0)
            {
                _writeStream.Position = 0;
                MultiPartUploader.UploadPart(_writeStream, _writeStream.Length);
            }
        }

        MultiPartUploader.CompleteUpload();
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
