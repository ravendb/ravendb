using System;
using System.IO;
using System.IO.Compression;
using ICSharpCode.SharpZipLib.Zip;
using Sparrow.Backups;
using Sparrow.Utils;

namespace Voron.Impl.Backup;

public sealed class BackupZipArchiveEntry
{
    private readonly ZipOutputStream _zipStream;
    private readonly SnapshotBackupCompressionAlgorithm _compressionAlgorithm;
    private readonly CompressionLevel _compressionLevel;

    public BackupZipArchiveEntry(ZipOutputStream zipStream, SnapshotBackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        _zipStream = zipStream ?? throw new ArgumentNullException(nameof(zipStream));
        _compressionAlgorithm = compressionAlgorithm;
        _compressionLevel = compressionLevel;
    }

    public Stream Open()
    {
        var stream = new ZipEntryOutputStream(_zipStream);

        switch (_compressionAlgorithm)
        {
            case SnapshotBackupCompressionAlgorithm.Zstd:
                if (_compressionLevel == CompressionLevel.NoCompression)
                    return stream;
                return ZstdStream.Compress(stream, _compressionLevel);
            case SnapshotBackupCompressionAlgorithm.Deflate:
                return stream;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }

    private sealed class ZipEntryOutputStream(ZipOutputStream zipStream) : Stream
    {
        private bool _disposed;

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => true;
        public override long Length => throw new NotSupportedException();
        public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

        public override void Flush() => zipStream.Flush();
        public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
        public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
        public override void SetLength(long value) => throw new NotSupportedException();
        
        public override void Write(byte[] buffer, int offset, int count) => zipStream.Write(buffer, offset, count);
        public override void Write(ReadOnlySpan<byte> buffer) => zipStream.Write(buffer);

        protected override void Dispose(bool disposing)
        {
            if (_disposed)
                return;
            
            _disposed = true;
            
            if (disposing) 
                zipStream.CloseEntry();
            
            base.Dispose(disposing);
        }
    }
}
