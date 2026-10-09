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
    private readonly int _zstdWorkers;

    /// <param name="zstdWorkers">Worker threads compressing each Zstd entry, 0 compresses on the calling thread.</param>
    public BackupZipArchive(ZipArchive zipArchive, SnapshotBackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel, int zstdWorkers = 0)
    {
        _zipArchive = zipArchive ?? throw new ArgumentNullException(nameof(zipArchive));
        _compressionAlgorithm = compressionAlgorithm;
        _compressionLevel = compressionLevel;
        _compressionLevelForZipEntry = GetCompressionLevelForZipEntry(compressionAlgorithm, compressionLevel);
        _zstdWorkers = zstdWorkers;
    }

    public BackupZipArchiveEntry CreateEntry(string entryName)
    {
        var zipEntry = _zipArchive.CreateEntry(entryName, _compressionLevelForZipEntry);
        return new BackupZipArchiveEntry(zipEntry, _compressionAlgorithm, _compressionLevel, _zstdWorkers);
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
