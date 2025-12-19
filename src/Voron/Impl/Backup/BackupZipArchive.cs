using System;
using System.IO.Compression;
using ICSharpCode.SharpZipLib.Zip;
using Sparrow.Backups;

namespace Voron.Impl.Backup;

public sealed class BackupZipArchive : IDisposable
{
    private readonly ZipOutputStream _zipStream;
    private readonly SnapshotBackupCompressionAlgorithm _compressionAlgorithm;
    private readonly CompressionLevel _compressionLevel;

    public BackupZipArchive(ZipOutputStream zipStream, SnapshotBackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        _zipStream = zipStream ?? throw new ArgumentNullException(nameof(zipStream));
        _compressionAlgorithm = compressionAlgorithm;
        _compressionLevel = compressionLevel;
        
        var level = GetCompressionLevel(compressionAlgorithm, compressionLevel);
        _zipStream.SetLevel(level);
    }

    public BackupZipArchiveEntry CreateEntry(string entryName, bool noCompression = false)
    {
        var zipEntry = new ZipEntry(entryName);

        if (noCompression)
            zipEntry.CompressionMethod = CompressionMethod.Stored;
        
        _zipStream.PutNextEntry(zipEntry);

        return new BackupZipArchiveEntry(_zipStream, _compressionAlgorithm, noCompression ? CompressionLevel.NoCompression : _compressionLevel);
    }

    private static int GetCompressionLevel(SnapshotBackupCompressionAlgorithm compressionAlgorithm, CompressionLevel compressionLevel)
    {
        return compressionAlgorithm switch
        {
            SnapshotBackupCompressionAlgorithm.Deflate => compressionLevel switch
            {
                CompressionLevel.NoCompression => 0,
                CompressionLevel.Fastest => 1,
                CompressionLevel.Optimal => 6,
                CompressionLevel.SmallestSize => 9,
                _ => throw new ArgumentOutOfRangeException(nameof(compressionLevel), compressionLevel, null)
            },
            SnapshotBackupCompressionAlgorithm.Zstd => 0,
            _ => throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null)
        };
    }

    public void Dispose()
    {
        _zipStream.Finish();
        _zipStream.Dispose();
    }
}
