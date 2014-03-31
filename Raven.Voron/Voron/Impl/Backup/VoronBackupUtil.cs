using System.Diagnostics;
using System.IO.Compression;
using Voron.Impl.FileHeaders;
using Voron.Util;

namespace Voron.Impl.Backup
{
    internal static unsafe class VoronBackupUtil
    {
        internal static void CopyHeaders(CompressionLevel compression, ZipArchive package, DataCopier copier, StorageEnvironmentOptions storageEnvironmentOptions)
        {
            foreach (var headerFileName in HeaderAccessor.HeaderFileNames)
            {
                var header = stackalloc FileHeader[1];

                if (!storageEnvironmentOptions.ReadHeader(headerFileName, header))
                    continue;

                var headerPart = package.CreateEntry(headerFileName, compression);
                Debug.Assert(headerPart != null);

                using (var headerStream = headerPart.Open())
                {
                    copier.ToStream((byte*)header, sizeof(FileHeader), headerStream);
                }
            }
        }
    }
}