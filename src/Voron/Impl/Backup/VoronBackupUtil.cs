using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using Voron.Impl.FileHeaders;
using Voron.Util;

namespace Voron.Impl.Backup
{
    internal static unsafe class VoronBackupUtil
    {
        internal static void CopyHeaders(CompressionLevel compression, ZipArchive package, DataCopier copier, StorageEnvironmentOptions storageEnvironmentOptions, string basePath)
        {
            foreach (var headerFileName in HeaderAccessor.HeaderFileNames)
            {
                var header = stackalloc FileHeader[1];

                if (!storageEnvironmentOptions.ReadHeader(headerFileName, header))
                    continue;

                var hash = HeaderAccessor.CalculateFileHeaderHash(header);
                if (header->Hash != hash)
                    throw new InvalidDataException($"Invalid hash for FileHeader with TransactionId {header->TransactionId}, possible corruption. Expected hash to be {header->Hash} but was {hash}");

                var headerPart = package.CreateEntry(Path.Combine(basePath,headerFileName), compression);
                Debug.Assert(headerPart != null);

                using (var headerStream = headerPart.Open())
                {
                    copier.ToStream((byte*)header, sizeof(FileHeader), headerStream);
                }
            }
        }
    }
}
