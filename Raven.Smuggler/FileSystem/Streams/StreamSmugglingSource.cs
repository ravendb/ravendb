// -----------------------------------------------------------------------
//  <copyright file="StreamSmugglingSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Smuggler.FileSystem.Streams
{
    public class StreamSmugglingSource : IFileSystemSmugglerSource
    {
        private readonly Stream stream;

        private readonly bool leaveOpen;

        private readonly JsonSerializer serializer;

        private FileSystemSmugglerOptions options;

        private ZipArchive archive;

        private Dictionary<string, ZipArchiveEntry> zipEntries;

        public StreamSmugglingSource(Stream stream, bool leaveOpen = false)
        {
            this.stream = stream;
            this.leaveOpen = leaveOpen;
            serializer = JsonExtensions.CreateDefaultJsonSerializer();
        }

        public string DisplayName { get; }

        public Task InitializeAsync(FileSystemSmugglerOptions options, CancellationToken cancellationToken)
        {
            this.options = options; // TODO arek - verify if options are really necessary in smuggling source

            try
            {
                archive = new ZipArchive(stream, ZipArchiveMode.Read);
            }
            catch (InvalidDataException e)
            {
                throw new InvalidDataException("Invalid file system export file", e);
            }

            zipEntries = archive.Entries.ToDictionary(x => x.FullName);

            return new CompletedTask();
        }

        public Task<IAsyncEnumerator<FileHeader>> GetFilesAsync(Etag lastEtag, int take, CancellationToken cancellationToken)
        {
            var fileHeaders = GetFilesInternal(lastEtag, take, cancellationToken);

            return new CompletedTask<IAsyncEnumerator<FileHeader>>(new AsyncEnumeratorBridge<FileHeader>(fileHeaders.GetEnumerator()));
        }

        private IEnumerable<FileHeader> GetFilesInternal(Etag lastEtag, int take, CancellationToken cancellationToken)
        {
            var metadataEntry = zipEntries[SmugglerConstants.FileSystem.MetadataEntry];

            var returned = 0;

            using (var streamReader = new StreamReader(metadataEntry.Open()))
            {
                foreach (var json in streamReader.EnumerateJsonObjects())
                {
                    var entry = serializer.Deserialize<FileEntry>(new StringReader(json));

                    var file = new FileHeader(entry.Key, entry.Metadata);

                    if (file.Etag.CompareTo(lastEtag) <= 0) // TODO arek - add test which checks that all files are smuggled even if BatchSize is very small
                        continue;

                    yield return file;

                    if (++returned >= take)
                        yield break;

                    cancellationToken.ThrowIfCancellationRequested();
                }
            }
        }

        public Task<IEnumerable<KeyValuePair<string, RavenJObject>>> GetConfigurations(int skip, int take)
        {
            var configs = GetConfigsInternal(skip, take);

            return new CompletedTask<IEnumerable<KeyValuePair<string, RavenJObject>>>(configs);
        }

        private IEnumerable<KeyValuePair<string, RavenJObject>> GetConfigsInternal(int skip, int take)
        {
            ZipArchiveEntry configurationsEntry;

            if (zipEntries.TryGetValue(SmugglerConstants.FileSystem.ConfigurationsEntry, out configurationsEntry) == false)
            {
                yield break;
            }

            var skipped = 0;
            var returned = 0;

            using (var streamReader = new StreamReader(configurationsEntry.Open()))
            {
                foreach (var json in streamReader.EnumerateJsonObjects())
                {
                    if (skipped++ < skip) // TODO arek - add test which checks that all configs are smuggled even if BatchSize is very small
                        continue;

                    var config = serializer.Deserialize<ConfigEntry>(new StringReader(json));

                    yield return new KeyValuePair<string, RavenJObject>(config.Name, config.Value);

                    if (++returned >= take)
                        yield break;
                }
            }
        }

        public Task<Stream> DownloadFileAsync(FileHeader file)
        {
            var entry = zipEntries[file.FullPath];

            return new CompletedTask<Stream>(entry.Open());
        }

        public Task<LastFilesEtagsInfo> FetchCurrentMaxEtagsAsync()
        {
            return new CompletedTask<LastFilesEtagsInfo>(new LastFilesEtagsInfo
                                     {
                                         LastFileEtag = null,
                                         LastDeletedFileEtag = null
                                     });
        }

        public IEnumerable<SmuggleType> GetItemsToSmuggle()
        {
            yield return SmuggleType.File;
            yield return SmuggleType.Configuration;
        }

        public void Dispose()
        {
            archive.Dispose();

            if (leaveOpen)
                return;
            
            stream?.Dispose();
        }
    }
}