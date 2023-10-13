using System;
using System.IO;
using System.IO.Compression;
using Sparrow.Backups;
using Sparrow.Utils;

namespace Voron.Impl.Backup;

public sealed class BackupZipArchiveEntry
{
    private readonly ZipArchiveEntry _zipEntry;
    private readonly BackupCompressionAlgorithm _compressionAlgorithm;
    private readonly CompressionLevel _compressionLevel;

    public BackupZipArchiveEntry(ZipArchiveEntry zipEntry, BackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        _zipEntry = zipEntry;
        _compressionAlgorithm = compressionAlgorithm;
        _compressionLevel = compressionLevel;
    }

    public Stream Open()
    {
        var stream = _zipEntry.Open();

        switch (_compressionAlgorithm)
        {
            case BackupCompressionAlgorithm.Zstd:
                if (_compressionLevel == CompressionLevel.NoCompression)
                    return stream;

                return ZstdStream.Compress(stream, _compressionLevel);
            case BackupCompressionAlgorithm.Gzip:
                return stream;
            default:
                throw new ArgumentOutOfRangeException();
        }
    }
}
