// -----------------------------------------------------------------------
//  <copyright file="SmuggleFilesToDestinationStreamImpl.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Threading.Tasks;

using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem.Streams
{
    public class SmuggleFilesToStream : ISmuggleFilesToDestination
    {
        private readonly ZipArchive archive;
        private readonly List<FileEntry> metadataList = new List<FileEntry>();

        public SmuggleFilesToStream(ZipArchive archive)
        {
            this.archive = archive;
        }

        public async Task WriteFileAsync(FileHeader file, Stream content)
        {
            // Write the metadata (which includes the stream size and file container name)
            var fileContainer = new FileEntry
            {
                Key = Path.Combine(file.Directory.TrimStart('/'), file.Name),
                Metadata = file.Metadata,
            };

            var zipArchiveEntry = archive.CreateEntry(fileContainer.Key);

            using (var zipEntryStream = zipArchiveEntry.Open())
            {
                await content.CopyToAsync(zipEntryStream).ConfigureAwait(false);
            }

            metadataList.Add(fileContainer);
        }

        public void Dispose()
        {
            var metadataEntry = archive.CreateEntry(SmugglerConstants.FileSystem.MetadataEntry);

            using (var metadataStream = metadataEntry.Open())
            using (var writer = new StreamWriter(metadataStream))
            {
                foreach (var item in metadataList)
                    writer.WriteLine(RavenJObject.FromObject(item));
            }
        }
    }
}