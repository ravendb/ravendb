using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Abstractions.Util;

namespace Raven.Client.FileSystem
{
    public class AsyncFilesSession : InMemoryFilesSessionOperations, IAsyncFilesSession, IAsyncAdvancedFilesSessionOperations
    {
        private IDisposable conflictCacheRemoval; 

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncFilesSession"/> class.
        /// </summary>
        public AsyncFilesSession(FilesStore filesStore,
                                 IAsyncFilesCommands asyncFilesCommands,
                                 FilesSessionListeners listeners,
                                 Guid id)
            : base(filesStore, listeners, id)
        {
            Commands = asyncFilesCommands;
            conflictCacheRemoval = filesStore.Changes(this.FileSystemName)
                                             .ForConflicts()
                                             .Subscribe(new FileConflictObserver(this));
        }

        /// <summary>
        /// Gets the async files commands.
        /// </summary>
        /// <value>The async files commands.</value>
        public IAsyncFilesCommands Commands { get; private set; }

        public override string FileSystemName
        {
            get { return Commands.FileSystemName; }
        }

        public IAsyncAdvancedFilesSessionOperations Advanced
        {
            get { return this; }
        }

        public IAsyncFilesQuery<FileHeader> Query()
        {
            return new AsyncFilesQuery<FileHeader>(this, this.Commands);
        }

        public async Task<FileHeader> LoadFileAsync(string filename)
        {
            if (string.IsNullOrWhiteSpace(filename))
                throw new ArgumentNullException("filename", "The filename cannot be null, empty or whitespace.");

            FileHeader existingEntity;
            if (entitiesByKey.TryGetValue(filename, out existingEntity))
            {
                // Check if the file is not currently been scheduled for deletion or known to be non-existent.
                if (!this.IsDeleted(filename))
                    return existingEntity;
                else
                    return null;
            }

            IncrementRequestCount();            

            // Check if the file exists on the server.
            var metadata = await Commands.GetMetadataForAsync(filename).ConfigureAwait(false);
            if (metadata == null)
                return null;

            var fileHeader = new FileHeader(filename, metadata);
            AddToCache(filename, fileHeader);

            return fileHeader;
        }



        public async Task<FileHeader[]> LoadFileAsync(IEnumerable<string> filenames)
        {
            if (!filenames.Any())
                return new FileHeader[0];

            filenames = filenames.Select(FileHeader.Canonize);

            // only load documents that aren't already cached
            var idsOfNotExistingObjects = filenames.Where(x => IsLoaded(x) == false && IsDeleted(x) == false)
                                            .Distinct(StringComparer.OrdinalIgnoreCase)
                                            .ToArray();

            if (idsOfNotExistingObjects.Length > 0)
            {
                IncrementRequestCount();

                var fileHeaders = await Commands.GetAsync(idsOfNotExistingObjects.ToArray()).ConfigureAwait(false);                                
                foreach( var header in fileHeaders )
                    AddToCache(header.FullPath, header);                
            }

            var result = new List<FileHeader>();
            foreach ( var file in filenames )
            {
                FileHeader obj = null;
                entitiesByKey.TryGetValue(file, out obj);
                result.Add( obj as FileHeader );
            }
            return result.ToArray();
        }

        public Task<Stream> DownloadAsync(string filename, Reference<RavenJObject> metadata = null)
        {
            if (string.IsNullOrWhiteSpace(filename))
                throw new ArgumentNullException("filename", "The filename cannot be null, empty or whitespace.");

            IncrementRequestCount();

            return Commands.DownloadAsync(filename, metadata);
            
        }

        public Task<Stream> DownloadAsync(FileHeader fileHeader, Reference<RavenJObject> metadata = null)
        {
            if (fileHeader == null || string.IsNullOrWhiteSpace(fileHeader.FullPath))
                throw new ArgumentNullException("fileHeader", "The file header cannot be null, and must have a filename.");

            return this.DownloadAsync(fileHeader.FullPath, metadata);
        }

        public async Task<FileHeader[]> LoadFilesAtDirectoryAsync(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentNullException("directory", "The directory cannot be null, empty or whitespace.");

            IncrementRequestCount();

            // TODO: Check this.
            var directoryName = directory.StartsWith("/") ? directory : "/" + directory;
            var searchResults = await Commands.SearchOnDirectoryAsync(directory).ConfigureAwait(false);
            return searchResults.Files.ToArray();
        }



        public class FileConflictObserver : IObserver<ConflictNotification>
        {
            private readonly AsyncFilesSession asyncFilesSession;

            public FileConflictObserver(AsyncFilesSession asyncFilesSession)
            {
                this.asyncFilesSession = asyncFilesSession;
            }

            public void OnNext(ConflictNotification notification)
            {
                if (notification.Status == ConflictStatus.Detected)
                {
                    asyncFilesSession.conflicts.Add(notification.FileName);
                }
                else
                {
                    asyncFilesSession.entitiesByKey.Remove(notification.FileName);
                    asyncFilesSession.conflicts.Remove(notification.FileName);
                }
            }

            public void OnError(Exception error)
            {
            }

            public void OnCompleted()
            {
            }
        }

        public override void Dispose ()
        {
            base.Dispose();

            if (this.conflictCacheRemoval != null)
                this.conflictCacheRemoval.Dispose();
        }

        public Task<IAsyncEnumerator<FileHeader>> StreamFileHeadersAsync(Etag fromEtag, int pageSize = int.MaxValue)
        {
            IncrementRequestCount();

            return Commands.StreamFileHeadersAsync(fromEtag, pageSize);
        }

        public Task<IAsyncEnumerator<FileHeader>> StreamQueryAsync(IAsyncFilesQuery<FileHeader> query)
        {
            IncrementRequestCount();

            var filesQuery = query.GetFilesQuery();

            return Commands.StreamQueryAsync(filesQuery.Query, filesQuery.SortFields, filesQuery.Start, filesQuery.PageSizeSet ? filesQuery.PageSize : int.MaxValue);
        }
    }
}
