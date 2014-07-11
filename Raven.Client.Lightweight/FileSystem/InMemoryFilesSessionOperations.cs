using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.FileSystem.Notifications;
using Raven.Abstractions.Logging;
using Raven.Client.FileSystem.Impl;
using Raven.Client.Util;
using Raven.Json.Linq;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.IO;
using System.IO.Pipes;
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
        /// Translate between a key and its associated entity
        /// </summary>
        protected readonly Dictionary<string, object> entitiesByKey = new Dictionary<string, object>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// The files waiting to be deleted
        /// </summary>
        protected readonly HashSet<object> deletedEntities = new HashSet<object>(ObjectReferenceEqualityComparer<object>.Default);

        /// <summary>
        /// Entities whose filename we already know do not exist, because they were registered for deletion, etc.
        /// </summary>
        protected readonly HashSet<string> knownMissingIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Current file conflicts
        /// </summary>
        protected readonly HashSet<string> conflicts = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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
        protected InMemoryFilesSessionOperations( FilesStore filesStore, FilesSessionListeners listeners, Guid id)
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
            var operation = new UploadFileOperation(this, path, stream.Length, x => { stream.CopyTo(x); }, metadata, etag); 
            registeredOperations.Enqueue(operation);   
        }

        public void RegisterUpload(FileHeader file, Stream stream, RavenJObject metadata = null, Etag etag = null)
        {
            var operation = new UploadFileOperation(this, file.Path, stream.Length, x => { stream.CopyTo(x); }, metadata, etag);
            registeredOperations.Enqueue(operation);   
        }

        public void RegisterUpload(string path, long size, Action<Stream> write, RavenJObject metadata = null, Etag etag = null)
        {
            var operation = new UploadFileOperation(this, path, size, write, metadata, etag);
            registeredOperations.Enqueue(operation);           
        }

        public void RegisterUpload(FileHeader file, long size, Action<Stream> write, RavenJObject metadata = null, Etag etag = null)
        {
            var operation = new UploadFileOperation(this, file.Path, size, write, metadata, etag);
            registeredOperations.Enqueue(operation);     
        }

        public void RegisterFileDeletion(string path, Etag etag = null)
        {
            var operation = new DeleteFileOperation(this, path, etag);
            registeredOperations.Enqueue(operation); 
        }

        public void RegisterFileDeletion(FileHeader file, Etag etag = null)
        {
            var operation = new DeleteFileOperation(this, file.Path, etag);
            registeredOperations.Enqueue(operation); 
        }

        public void RegisterRename(string sourceFile, string destinationFile)
        {
            var operation = new RenameFileOperation(this, sourceFile, destinationFile);
            registeredOperations.Enqueue(operation);
        }

        public void RegisterRename(FileHeader sourceFile, string destinationFile)
        {
            RegisterRename(sourceFile.Path, destinationFile);
        }

        public void AddToCache(string filename, FileHeader fileHeader)
        {
            entitiesByKey.Add(filename, fileHeader);
            if (this.IsDeleted(filename))
                knownMissingIds.Remove(filename);
        }

        /// <summary>
        /// Returns whatever a filename with the specified id is loaded in the 
        /// current session
        /// </summary>
        public bool IsLoaded(string id)
        {
            return entitiesByKey.ContainsKey(id);
        }

        /// <summary>
        /// Returns whatever a filename with the specified id is deleted.
        /// </summary>
        public bool IsDeleted(string id)
        {
            return knownMissingIds.Contains(id);
        }

        public void RegisterMissing(string id)
        {
            knownMissingIds.Add(id);
        }

        public async Task SaveChangesAsync()
        {
            var session = this as IAsyncFilesSession;
            if (session == null)
                throw new InvalidCastException("This object does not implement IAsyncFilesSession.");

            var operationsToExecute = this.registeredOperations; // Potential race condition in between this two, not thread safe.
            this.registeredOperations = new ConcurrentQueue<IFilesOperation>();

            IFilesOperation op;
            while (operationsToExecute.TryDequeue(out op))
            {
                AssertConflictsAreNotAffectingOperation(op);
                await op.Execute(session);
            }
        }

        private void AssertConflictsAreNotAffectingOperation(IFilesOperation operation)
        {
            string fileName = null;
            if (operation.GetType() == typeof(UploadFileOperation))
            {
                fileName = (operation as UploadFileOperation).Path;
            }
            else if (operation.GetType() == typeof(RenameFileOperation))
            {
                fileName = (operation as RenameFileOperation).Source;
            }

            if (fileName != null && conflicts.Contains(fileName))
                throw new NotSupportedException( string.Format("There is a conflict over file: {0}. Update or remove operations are not supported", fileName));
        }

        public void IncrementRequestCount()
        {
            if (++NumberOfRequests > MaxNumberOfRequestsPerSession)
                throw new InvalidOperationException(
                    string.Format(
                        @"The maximum number of requests ({0}) allowed for this session has been reached.
Raven limits the number of remote calls that a session is allowed to make as an early warning system. Sessions are expected to be short lived, and 
Raven provides facilities like Load(string[] path) to load multiple files at once and batch saves (call SaveChanges() only once).
You can increase the limit by setting FilesConvention.MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is
advisable that you'll look into reducing the number of remote calls first, since that will speed up your application significantly and result in a 
more responsive application.",
                        MaxNumberOfRequestsPerSession));
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
