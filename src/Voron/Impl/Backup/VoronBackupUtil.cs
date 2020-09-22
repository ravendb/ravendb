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
            var header = stackalloc FileHeader[1];
            var success = false;
            foreach (var headerFileName in HeaderAccessor.HeaderFileNames)
            {
                if (!storageEnvironmentOptions.ReadHeader(headerFileName, header))
                    continue;

                success = true;

                var headerPart = package.CreateEntry(Path.Combine(basePath, headerFileName), compression);
                Debug.Assert(headerPart != null);

                using (var headerStream = headerPart.Open())
                {
                    copier.ToStream((byte*)header, sizeof(FileHeader), headerStream);
                }
            }

            if (!success)
                throw new InvalidDataException($"Failed to read both file headers (headers.one & headers.two) from path: {basePath}, possible corruption.");
        }
    }
}
