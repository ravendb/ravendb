// -----------------------------------------------------------------------
//  <copyright file="FilesSmugglerEmbeddedSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;
using Raven.Smuggler.FileSystem;

namespace Raven.Database.FileSystem.Smuggler.Embedded
{
    public class EmbeddedSmugglingSource : IFileSystemSmugglerSource
    {
        private readonly RavenFileSystem fileSystem;
        
        public EmbeddedSmugglingSource(RavenFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
        }

        public string DisplayName => fileSystem.Name;

        public Task InitializeAsync(FileSystemSmugglerOptions options, CancellationToken cancellationToken)
        {
            return new CompletedTask();
        }

        public Task<IAsyncEnumerator<FileHeader>> GetFilesAsync(Etag lastEtag, int take, CancellationToken cancellationToken)
        {
            IList<FileHeader> results = null;

            fileSystem.Storage.Batch(accessor =>
            {
                results = accessor.GetFilesAfter(lastEtag, take).ToList();
            });

            return new CompletedTask<IAsyncEnumerator<FileHeader>>(new AsyncEnumeratorBridge<FileHeader>(results.GetEnumerator()));
        }

        public Task<IEnumerable<KeyValuePair<string, RavenJObject>>> GetConfigurations(int skip, int take)
        {
            var results = new List<KeyValuePair<string, RavenJObject>>();

            fileSystem.Storage.Batch(accessor =>
            {
                var names = accessor.GetConfigNames(skip, take);

                foreach (var name in names)
                {
                    results.Add(new KeyValuePair<string, RavenJObject>(name, accessor.GetConfig(name)));
                }
            });

            return new CompletedTask<IEnumerable<KeyValuePair<string, RavenJObject>>>(results);
        }

        public Task<Stream> DownloadFileAsync(FileHeader file)
        {
            var name = file.FullPath;
            var readingStream = StorageStream.Reading(fileSystem.Storage, name);
            return new CompletedTask<Stream>(readingStream);
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
        }
    }
}