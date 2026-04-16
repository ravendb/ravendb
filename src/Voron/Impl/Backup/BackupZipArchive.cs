using System;
using System.IO.Compression;
using Sparrow.Backups;

namespace Voron.Impl.Backup;

public sealed class BackupZipArchive
{
    private readonly ZipArchive _zipArchive;
    private readonly SnapshotBackupCompressionAlgorithm _compressionAlgorithm;
    private readonly CompressionLevel _compressionLevel;
    private readonly CompressionLevel _compressionLevelForZipEntry;

    public BackupZipArchive(ZipArchive zipArchive, SnapshotBackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        _zipArchive = zipArchive ?? throw new ArgumentNullException(nameof(zipArchive));
        _compressionAlgorithm = compressionAlgorithm;
        _compressionLevel = compressionLevel;
        _compressionLevelForZipEntry = GetCompressionLevelForZipEntry(compressionAlgorithm, compressionLevel);
    }

    public BackupZipArchiveEntry CreateEntry(string entryName)
    {
        var zipEntry = _zipArchive.CreateEntry(entryName, _compressionLevelForZipEntry);
        return new BackupZipArchiveEntry(zipEntry, _compressionAlgorithm, _compressionLevel);
    }

    private static CompressionLevel GetCompressionLevelForZipEntry(SnapshotBackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        return compressionAlgorithm switch
        {
            SnapshotBackupCompressionAlgorithm.Zstd => CompressionLevel.NoCompression,
            SnapshotBackupCompressionAlgorithm.Deflate => compressionLevel,
            _ => throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null)
        };
    }
}
