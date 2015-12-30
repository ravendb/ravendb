// -----------------------------------------------------------------------
//  <copyright file="RemoteSmugglingSource.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Data;
using Raven.Abstractions.Database.Smuggler.FileSystem;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Client.FileSystem;
using Raven.Json.Linq;
using Raven.Smuggler.Helpers;

namespace Raven.Smuggler.FileSystem.Remote
{
    public class RemoteSmugglingSource : IFileSystemSmugglerSource
    {
        private readonly FilesConnectionStringOptions connectionOptions;

        private FilesStore filesStore;

        public RemoteSmugglingSource(FilesConnectionStringOptions connectionOptions)
        {
            this.connectionOptions = connectionOptions;
        }

        public string DisplayName { get; }

        public async Task InitializeAsync(FileSystemSmugglerOptions options, CancellationToken cancellationToken)
        {
            filesStore = FileStoreHelper.CreateStore(connectionOptions);

            await ServerValidation.ValidateThatServerIsUpAndFileSystemExists(connectionOptions, filesStore).ConfigureAwait(false);

            await ServerValidation.DetectServerSupportedFeatures(connectionOptions).ConfigureAwait(false); // TODO arek - merge those 2 methods into single one
        }

        public Task<IAsyncEnumerator<FileHeader>> GetFilesAsync(Etag lastEtag, int take, CancellationToken cancellationToken)
        {
            return filesStore.AsyncFilesCommands.StreamFileHeadersAsync(lastEtag, pageSize: take);
        }

        public async Task<IEnumerable<KeyValuePair<string, RavenJObject>>> GetConfigurations(int skip, int take)
        {
            var names = await filesStore.AsyncFilesCommands.Configuration.GetKeyNamesAsync(skip, take).ConfigureAwait(false);

            var results = new List<KeyValuePair<string, RavenJObject>>(names.Length);

            foreach (var name in names)
            {
                results.Add(new KeyValuePair<string, RavenJObject>(name, await filesStore.AsyncFilesCommands.Configuration.GetKeyAsync<RavenJObject>(name).ConfigureAwait(false)));
            }

            return results;
        }

        public Task<Stream> DownloadFileAsync(FileHeader file)
        {
            return filesStore.AsyncFilesCommands.DownloadAsync(file.FullPath);
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
            filesStore?.Dispose();
        }
    }
}