﻿using System;
using Raven.Client.Documents.Smuggler;
using Sparrow.Backups;

namespace Raven.Server.Documents.PeriodicBackup;

internal static class BackupCompressionAlgorithmExtensions
{
    public static ExportCompressionAlgorithm ToExportCompressionAlgorithm(this BackupCompressionAlgorithm compressionAlgorithm)
    {
        switch (compressionAlgorithm)
        {
            case BackupCompressionAlgorithm.Zstd:
                return ExportCompressionAlgorithm.Zstd;
            case BackupCompressionAlgorithm.Gzip:
                return ExportCompressionAlgorithm.Gzip;
            default:
                throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null);
        }
    }

    public static ExportCompressionAlgorithm ToExportCompressionAlgorithm(this SnapshotBackupCompressionAlgorithm compressionAlgorithm)
    {
        switch (compressionAlgorithm)
        {
            case SnapshotBackupCompressionAlgorithm.Zstd:
                return ExportCompressionAlgorithm.Zstd;
            case SnapshotBackupCompressionAlgorithm.Deflate:
                return ExportCompressionAlgorithm.Gzip;
            default:
                throw new ArgumentOutOfRangeException(nameof(compressionAlgorithm), compressionAlgorithm, null);
        }
    }
}
