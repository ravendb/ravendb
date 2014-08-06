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

        private IDisposable sessionChangesSubscription;

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
            sessionChangesSubscription = filesStore.Changes(null, this).ForConflicts().Subscribe();
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

            IncrementRequestCount();

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

            IncrementRequestCount();

            var directoryName = directory.StartsWith("/") ? directory : "/" + directory;
            var searchResults = await Commands.SearchOnDirectoryAsync(directory);
            return searchResults.Files.ToArray();
        }

        void IObserver<ConflictNotification>.OnNext(ConflictNotification notification)
        {
            if (!conflicts.Contains(notification.FileName))
                conflicts.Add(notification.FileName);

            // IMPORTANT: Going async here will break the &4.2 reactive extension guideline and more importantly
            //            introduce a race condition in the handling of conflict listeners at the client side.
            //            http://go.microsoft.com/fwlink/?LinkID=205219
            var localHeader = this.LoadFileAsync(notification.FileName).Result;

            if (notification.Status == ConflictStatus.Detected) 
            {                               
                int actionableListenersCount = 0;
                var resolutionStrategy = ConflictResolutionStrategy.NoResolution;
                foreach( var listener in Listeners.ConflictListeners)
                {
                    var strategy = listener.ConflictDetected(localHeader, notification.RemoteFileHeader, notification.SourceServerUrl);
                    if (strategy != ConflictResolutionStrategy.NoResolution )
                    {
                        if (actionableListenersCount > 0)
                            return;
                        
                        if (actionableListenersCount == 0) 
                        {
                            actionableListenersCount++;
                            resolutionStrategy = strategy;
                        }
                    }
                }

                Commands.Synchronization.ResolveConflictAsync(localHeader.Name, resolutionStrategy).Wait();
            }
            else
            {
                if (entitiesByKey.ContainsKey(notification.FileName))
                    entitiesByKey.Remove(notification.FileName);

                if (conflicts.Contains(notification.FileName))
                    conflicts.Remove(notification.FileName);

                foreach (var listener in Listeners.ConflictListeners)
                    listener.ConflictResolved(localHeader);
            }
        }

        void IObserver<ConflictNotification>.OnError(Exception error)
        {
        }

        void IObserver<ConflictNotification>.OnCompleted()
        {
        }

        public override void Dispose ()
        {
            base.Dispose();

            if (sessionChangesSubscription != null)
                sessionChangesSubscription.Dispose();
        }
    }
}
