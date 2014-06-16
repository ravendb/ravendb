using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Client.FileSystem.Impl;
using Raven.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Raven.Client.FileSystem
{
    public abstract class InMemoryFilesSessionOperations : IDisposable
    {
        private static int counter;
        private readonly int hash = Interlocked.Increment(ref counter);

        /// <summary>
        /// The session id 
        /// </summary>
        public Guid Id { get; private set; }

        /// <summary>
        /// The file system name for this session
        /// </summary>
        public abstract string FileSystemName { get; }

        protected static readonly ILog log = LogManager.GetCurrentClassLogger();

        protected readonly string fsName;
        private readonly FilesStore filesStore;


        /// <summary>
        /// all the listeners for this session
        /// </summary>
        protected readonly FilesSessionListeners theListeners;

        /// <summary>
        /// all the listeners for this session
        /// </summary>
        public FilesSessionListeners Listeners
        {
            get { return theListeners; }
        }

        ///<summary>
        /// The files store associated with this session
        ///</summary>
        public IFilesStore FilesStore
        {
            get { return filesStore; }
        }

        /// <summary>
        /// Gets the number of requests for this session
        /// </summary>
        /// <value></value>
        public int NumberOfRequests { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryFilesSessionOperations"/> class.
		/// </summary>
        protected InMemoryFilesSessionOperations(
            FilesStore filesStore,
            FilesSessionListeners listeners,
			Guid id)
		{
            this.Id = id;
            this.filesStore = filesStore;
            this.theListeners = listeners;            

            this.MaxNumberOfRequestsPerSession = filesStore.Conventions.MaxNumberOfRequestsPerSession;			
		}

        /// <summary>
        /// Gets the store identifier for this session.
        /// The store identifier is the identifier for the particular RavenDB instance.
        /// </summary>
        /// <value>The store identifier.</value>
        public string StoreIdentifier
        {
            get { return filesStore.Identifier + ";" + FileSystemName; }
        }



        /// <summary>
        /// Gets the conventions used by this session
        /// </summary>
        /// <value>The conventions.</value>
        /// <remarks>
        /// This instance is shared among all sessions, changes to the <see cref="FilesConvention"/> should be done
        /// via the <see cref="IFilesStore"/> instance, not on a single session.
        /// </remarks>
        public FilesConvention Conventions
        {
            get { return filesStore.Conventions; }
        }

        /// <summary>
        /// Gets or sets the max number of requests per session.
        /// If the <see cref="NumberOfRequests"/> rise above <see cref="MaxNumberOfRequestsPerSession"/>, an exception will be thrown.
        /// </summary>
        /// <value>The max number of requests per session.</value>
        public int MaxNumberOfRequestsPerSession { get; set; }


        private ConcurrentQueue<IFilesOperation> registeredOperations = new ConcurrentQueue<IFilesOperation>();
        

        public void RegisterUpload(string path, Stream stream, RavenJObject metadata = null, Etag etag = null)
        {                    
            throw new NotImplementedException();
        }

        public void RegisterUpload(FileHeader path, Stream stream, RavenJObject metadata = null, Etag etag = null)
        {
            throw new NotImplementedException();
        }

        public void RegisterUpload(string path, Action<Stream> write, RavenJObject metadata = null, Etag etag = null)
        {
            var operation = new UploadFileOperation(path, write, metadata, etag);
            registeredOperations.Enqueue(operation);           
        }

        public void RegisterUpload(FileHeader file, Action<Stream> write, RavenJObject metadata = null, Etag etag = null)
        {
            var operation = new UploadFileOperation(file.Path, write, metadata, etag);
            registeredOperations.Enqueue(operation);     
        }

        public void RegisterFileDeletion(string path, Etag etag = null)
        {
            var operation = new DeleteFileOperation(path, etag);
            registeredOperations.Enqueue(operation); 
        }

        public void RegisterFileDeletion(FileHeader file, Etag etag = null)
        {
            var operation = new DeleteFileOperation(file.Path, etag);
            registeredOperations.Enqueue(operation); 
        }

        public void RegisterDirectoryDeletion(string path, bool recurse = false)
        {
            var operation = new DeleteDirectoryOperation(path, recurse);
            registeredOperations.Enqueue(operation);   
        }

        public void RegisterDirectoryDeletion(DirectoryHeader directory, bool recurse = false)
        {
            var operation = new DeleteDirectoryOperation(directory.Path, recurse);
            registeredOperations.Enqueue(operation); 
        }

        public void RegisterRename(string sourceFile, string destinationFile)
        {
            var operation = new RenameFileOperation(sourceFile, destinationFile);
            registeredOperations.Enqueue(operation);
        }

        public void RegisterRename(FileHeader sourceFile, string destinationFile)
        {
            var operation = new RenameFileOperation(sourceFile.Path, destinationFile);
            registeredOperations.Enqueue(operation);
        }

        public void RegisterRename(FileHeader sourceFile, DirectoryHeader destination, string destinationName)
        {
            //TODO Validate destinationName is a filename and not a directory.
            var operation = new RenameFileOperation(sourceFile.Path, Path.Combine(destination.Path, destinationName));
            registeredOperations.Enqueue(operation);
        }

        public Task SaveChangesAsync()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public virtual void Dispose()
        {
        }


        public override int GetHashCode()
        {
            return hash;
        }

        public override bool Equals(object obj)
        {
            return ReferenceEquals(obj, this);
        }
    }
}
