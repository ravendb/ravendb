using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Json.Linq;
using System;
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


        public void RegisterUploadAsync(string path, Stream stream, RavenJObject metadata = null, Etag etag = null)
        {
            throw new NotImplementedException();
        }

        public void RegisterUploadAsync(FileHeader path, Stream stream, RavenJObject metadata = null, Etag etag = null)
        {
            throw new NotImplementedException();
        }

        public void RegisterUploadAsync(string path, long start, Action<Stream> write, RavenJObject metadata = null, Etag etag = null)
        {
            throw new NotImplementedException();
        }

        public void RegisterUploadAsync(FileHeader path, long start, Action<Stream> write, RavenJObject metadata = null, Etag etag = null)
        {
            throw new NotImplementedException();
        }

        public void RegisterFileDeletionAsync(string path, Etag etag = null)
        {
            throw new NotImplementedException();
        }

        public void RegisterFileDeletionAsync(FileHeader path, Etag etag = null)
        {
            throw new NotImplementedException();
        }

        public void RegisterDirectoryDeletionAsync(string path, bool recurse = false)
        {
            throw new NotImplementedException();
        }

        public void RegisterDirectoryDeletionAsync(DirectoryHeader path, bool recurse = false)
        {
            throw new NotImplementedException();
        }

        public void RegisterRenameAsync(string sourceFile, string destinationFile)
        {
            throw new NotImplementedException();
        }

        public void RegisterRenameAsync(FileHeader sourceFile, string destinationFile)
        {
            throw new NotImplementedException();
        }

        public void RegisterRenameAsync(FileHeader sourceFile, DirectoryHeader destinationPath, string destinationName)
        {
            throw new NotImplementedException();
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
