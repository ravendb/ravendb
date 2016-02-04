// -----------------------------------------------------------------------
//  <copyright file="FileSmugglingSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Json.Linq;
using Raven.Smuggler.FileSystem.Streams;

namespace Raven.Smuggler.FileSystem.Files
{
    public class FileSmugglingSource : IFileSystemSmugglerSource
    {
        private readonly string path;

        private int currentSource = 0;
        private readonly List<StreamSmugglingSource> sources;

        public FileSmugglingSource(string fileOrDirectoryPath)
        {
            path = Path.GetFullPath(fileOrDirectoryPath);
            sources = new List<StreamSmugglingSource>();
        }

        public void Dispose()
        {
            foreach (var source in sources)
                source.Dispose();
        }

        public string DisplayName { get; }

        public async Task InitializeAsync(FileSystemSmugglerOptions options, CancellationToken cancellationToken)
        {
            if (File.Exists(path))
            {
                sources.Add(await CreateSourceAsync(options, path, cancellationToken).ConfigureAwait(false));
                return;
            }

            // incremental

            var directory = new DirectoryInfo(path);
            if (!directory.Exists)
                throw new InvalidOperationException($"The directory '{path}' does not exists.");

            var files = Directory.GetFiles(directory.FullName)
                            .Where(file => Path.GetExtension(file).Equals(".ravenfs-incremental-dump", StringComparison.CurrentCultureIgnoreCase))
                            .OrderBy(x => File.GetLastWriteTimeUtc(x))
                            .ToArray();

            if (files.Length == 0)
                return;

            foreach (string filename in files)
            {
                var filePath = Path.Combine(path, filename);

                sources.Add(await CreateSourceAsync(options, filePath, cancellationToken).ConfigureAwait(false));
            }
        }

        public Task<IAsyncEnumerator<FileHeader>> GetFilesAsync(Etag lastEtag, int take, CancellationToken cancellationToken)
        {
            return sources[currentSource].GetFilesAsync(lastEtag, take, cancellationToken);
        }

        public Task<IEnumerable<KeyValuePair<string, RavenJObject>>> GetConfigurations(int skip, int take)
        {
            return sources[currentSource].GetConfigurations(skip, take);
        }

        public Task<Stream> DownloadFileAsync(FileHeader file)
        {
            return sources[currentSource].DownloadFileAsync(file);
        }

        public Task<LastFilesEtagsInfo> FetchCurrentMaxEtagsAsync()
        {
            return sources[currentSource].FetchCurrentMaxEtagsAsync();
        }

        public IEnumerable<SmuggleType> GetItemsToSmuggle()
        {
            for (var i = 0; i < sources.Count; i++)
            {
                yield return SmuggleType.File;
                yield return SmuggleType.Configuration;

                currentSource++;
            }
        }

        private static async Task<StreamSmugglingSource> CreateSourceAsync(FileSystemSmugglerOptions options, string path, CancellationToken cancellationToken)
        {
            var source = new StreamSmugglingSource(File.OpenRead(path))
            {
                // TODO arek DisplayName = Path.GetFileName(path)
            };

            await source
                .InitializeAsync(options, cancellationToken)
                .ConfigureAwait(false);

            return source;
        }
    }
}