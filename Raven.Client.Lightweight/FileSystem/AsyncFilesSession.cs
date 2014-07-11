using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.FileSystem.Notifications;

namespace Raven.Client.FileSystem
{
    public class AsyncFilesSession : InMemoryFilesSessionOperations, IAsyncFilesSession, IAsyncAdvancedFilesSessionOperations, IObserver<ConflictNotification>
    {
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
            filesStore.Changes().ForConflicts().Subscribe(this);
		}

        /// <summary>
        /// Gets the async files commands.
        /// </summary>
        /// <value>The async files commands.</value>
        public IAsyncFilesCommands Commands { get; private set; }

        public override string FileSystemName
        {
            get { return Commands.FileSystem; }
        }

        public IAsyncAdvancedFilesSessionOperations Advanced
        {
            get { return this; }
        }

        public async Task<FileHeader> LoadFileAsync(string filename)
        {
            if (string.IsNullOrWhiteSpace(filename))
                throw new ArgumentNullException("filename", "The filename cannot be null, empty or whitespace.");

            object existingEntity;
            if (entitiesByKey.TryGetValue(filename, out existingEntity))
            {
                // Check if the file is not currently been scheduled for deletion or known to be non-existent.
                if (!this.IsDeleted(filename))
                    return existingEntity as FileHeader;
                else
                    return null;
            }

            IncrementRequestCount();            

            // Check if the file exists on the server.
            var metadata = await Commands.GetMetadataForAsync(filename);
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

            // only load documents that aren't already cached
            var idsOfNotExistingObjects = filenames.Where(x => IsLoaded(x) == false && IsDeleted(x) == false)
                                            .Distinct(StringComparer.OrdinalIgnoreCase)
                                            .ToArray();

            if (idsOfNotExistingObjects.Length > 0)
            {
                IncrementRequestCount();

                var fileHeaders = await Commands.GetAsync(idsOfNotExistingObjects.ToArray());                                
                foreach( var header in fileHeaders )
                    AddToCache(header.Name, header);                
            }

            var result = new List<FileHeader>();
            foreach ( var file in filenames )
            {
                object obj = null;
                entitiesByKey.TryGetValue(file, out obj);
                result.Add( obj as FileHeader );
            }
            return result.ToArray();
        }

        public Task<Stream> DownloadAsync(string filename, Reference<RavenJObject> metadata = null)
        {
            if (string.IsNullOrWhiteSpace(filename))
                throw new ArgumentNullException("filename", "The filename cannot be null, empty or whitespace.");

            return Commands.DownloadAsync(filename, metadata);
            
        }

        public Task<Stream> DownloadAsync(FileHeader fileHeader, Reference<RavenJObject> metadata = null)
        {
            if (fileHeader == null || string.IsNullOrWhiteSpace(fileHeader.Name))
                throw new ArgumentNullException("fileHeader", "The file header cannot be null, and must have a filename.");

            return this.DownloadAsync(fileHeader.Name, metadata);
        }

        public async Task<FileHeader[]> LoadFilesAtDirectoryAsync(string directory)
        {
            if (string.IsNullOrWhiteSpace(directory))
                throw new ArgumentNullException("directory", "The directory cannot be null, empty or whitespace.");

            var directoryName = directory.StartsWith("/") ? directory : "/" + directory;
            var searchResults = await Commands.SearchOnDirectoryAsync(directory);
            return searchResults.Files.ToArray();
        }

        public async void OnNext(ConflictNotification notification)
        {
            if (!conflicts.Contains(notification.FileName))
                conflicts.Add(notification.FileName);
                
            var localHeader = await this.LoadFileAsync(notification.FileName);
            //var remolocalHeaderteHeader = await Commands.ForFileSystem(notification.SourceServerUrl).GetMetadataForAsync(notification.FileName);
            if (notification.Status == ConflictStatus.Detected) 
            {
                var resolutionStrategy = ConflictResolutionStrategy.NoResolution;
                int actionableListenersCount = 0;
                foreach( var listener in Listeners.ConflictListeners)
                {
                    var strategy = listener.ConflictDetected(localHeader, notification.RemoteFileHeader, notification.SourceServerUrl);

                    if (strategy != ConflictResolutionStrategy.NoResolution )
                    {
                        if (actionableListenersCount > 0)
                        {
                            return;
                        }
                        if (actionableListenersCount == 0) 
                        {
                            actionableListenersCount++;
                            resolutionStrategy = strategy;
                        }
                    }
                }

                await Commands.Synchronization.ResolveConflictAsync(localHeader.Name, resolutionStrategy);
            }
            else
            {
                callListenersOnConflictResolved(notification.FileName);  
            }
        }

        private async void callListenersOnConflictResolved(string fileName)
        {
            if (entitiesByKey.ContainsKey(fileName))
                entitiesByKey.Remove(fileName);

            if (conflicts.Contains(fileName))
                conflicts.Remove(fileName);

            var localHeader = await this.LoadFileAsync(fileName);
            foreach (var listener in Listeners.ConflictListeners)
            {
                listener.ConflictResolved(localHeader);
            }
        }

        public void OnError(Exception error)
        {
        }

        public void OnCompleted()
        {
        }
    }
}
