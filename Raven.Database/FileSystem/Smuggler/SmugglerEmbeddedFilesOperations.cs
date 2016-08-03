// -----------------------------------------------------------------------
//  <copyright file="SmugglerEmbeddedDatabaseOperations.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Smuggler;
using Raven.Abstractions.Smuggler.Data;
using Raven.Abstractions.Util;
using Raven.Database.FileSystem.Actions;
using Raven.Database.FileSystem.Util;
using Raven.Json.Linq;

namespace Raven.Database.FileSystem.Smuggler
{
    public class SmugglerEmbeddedFilesOperations : ISmugglerFilesOperations
    {

        private readonly RavenFileSystem filesystem;

        public SmugglerEmbeddedFilesOperations(RavenFileSystem filesystem)
        {
            this.filesystem = filesystem;
        }

        public Action<string> Progress { get; set; }

        public SmugglerFilesOptions Options { get; private set; }

        public Task<FileSystemStats[]> GetStats()
        {
            var count = 0;
            filesystem.Storage.Batch(accessor =>
            {
                count = accessor.GetFileCount();
            });

            var stats = new FileSystemStats
            {
                Name = filesystem.Name,
                FileSystemId = filesystem.Storage.Id,
                FileCount = count,
                Metrics = filesystem.CreateMetrics(),
                ActiveSyncs = filesystem.SynchronizationTask.Queue.Active.ToList(),
                PendingSyncs = filesystem.SynchronizationTask.Queue.Pending.ToList()
            };

            return new CompletedTask<FileSystemStats[]>(new [] {stats});
        }

        public Task<string> GetVersion(FilesConnectionStringOptions server)
        {
            return new CompletedTask<string>(DocumentDatabase.ProductVersion);
        }

        public LastFilesEtagsInfo FetchCurrentMaxEtags()
        {
            return new LastFilesEtagsInfo
            {
                LastFileEtag = null,
                LastDeletedFileEtag = null
            };
        }

        public Task<IAsyncEnumerator<FileHeader>> GetFiles(Etag lastEtag, int take)
        {
            ShowProgress("Streaming documents from {0}, batch size {1}", lastEtag, take);

            IEnumerable<FileHeader> enumerable = null;

            filesystem.Storage.Batch(accessor =>
            {
                enumerable = accessor.GetFilesAfter(lastEtag, take).ToList();
            });

            return new CompletedTask<IAsyncEnumerator<FileHeader>>(new AsyncEnumeratorBridge<FileHeader>(enumerable.GetEnumerator()));
        }

        public Task<IEnumerable<KeyValuePair<string, RavenJObject>>> GetConfigurations(int start, int take)
        {
            var results = new List<KeyValuePair<string, RavenJObject>>();

            filesystem.Storage.Batch(accessor =>
            {
                var names = accessor.GetConfigNames(start, take);

                foreach (var name in names)
                {
                    results.Add(new KeyValuePair<string, RavenJObject>(name, accessor.GetConfig(name)));
                }
            });

            return new CompletedTask<IEnumerable<KeyValuePair<string, RavenJObject>>>(results);
        }

        public Task PutConfig(string name, RavenJObject value)
        {
            filesystem.Storage.Batch(accessor => accessor.SetConfig(name, value));

            return new CompletedTask();
        }

        public Task<Stream> DownloadFile(FileHeader file)
        {
            var name = file.FullPath;
            var readingStream = StorageStream.Reading(filesystem.Storage, name);
            return new CompletedTask<Stream>(readingStream);
        }

        public async Task PutFile(FileHeader file, Stream data, long dataSize)
        {
            await filesystem.Files.PutAsync(file.FullPath, null, file.Metadata, () => new CompletedTask<Stream>(data), new FileActions.PutOperationOptions
            {
                ContentLength = dataSize
            }).ConfigureAwait(false);
        }

        public void Initialize(SmugglerFilesOptions options)
        {
            Options = options;
        }

        public void Configure(SmugglerFilesOptions options)
        {
        }

        public void ShowProgress(string format, params object[] args)
        {
            if (Progress != null)
            {
                Progress(string.Format(format, args));
            }
        }

        public string CreateIncrementalKey()
        {
            throw new NotSupportedException("Copying between filesystems is currently not supported in embedded version.");
        }

        public Task<ExportFilesDestinations> GetIncrementalExportKey()
        {
            throw new NotSupportedException("Copying between filesystems is currently not supported in embedded version.");
        }

        public Task PutIncrementalExportKey(ExportFilesDestinations destinations)
        {
            throw new NotSupportedException("Copying between filesystems is currently not supported in embedded version.");
        }

        public RavenJObject StripReplicationInformationFromMetadata(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata.Remove(SynchronizationConstants.RavenSynchronizationHistory);
                metadata.Remove(SynchronizationConstants.RavenSynchronizationSource);
                metadata.Remove(SynchronizationConstants.RavenSynchronizationVersion);
            }

            return metadata;
        }

        public RavenJObject DisableVersioning(RavenJObject metadata)
        {
            if (metadata != null)
            {
                metadata.Add(Constants.RavenIgnoreVersioning, true);
            }

            return metadata;
        }
    }
}
