using Raven.Abstractions.Data;
using Raven.Abstractions.FileSystem;
using Raven.Abstractions.Logging;
using Raven.Client.FileSystem.Impl;
using Raven.Json.Linq;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
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

#if !DNXCORE50
        private readonly static ILog log = LogManager.GetCurrentClassLogger();
#else
        private readonly static ILog log = LogManager.GetLogger(typeof(InMemoryFilesSessionOperations));
#endif

        protected readonly string fsName;
        private readonly FilesStore filesStore;



        /// <summary>
        /// Translate between a key and its associated entity
        /// </summary>
        protected readonly Dictionary<string, FileHeader> entitiesByKey = new Dictionary<string, FileHeader>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// The files waiting to be deleted
        /// </summary>
        protected readonly HashSet<string> deletedEntities = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// Entities whose filename we already know do not exist, because they were registered for deletion, etc.
        /// </summary>
        protected readonly HashSet<string> knownMissingIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

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

        public bool UseOptimisticConcurrency { get; set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="InMemoryFilesSessionOperations"/> class.
        /// </summary>
        protected InMemoryFilesSessionOperations( FilesStore filesStore, FilesSessionListeners listeners, Guid id)
        {
            Id = id;
            this.filesStore = filesStore;
            theListeners = listeners;            

            MaxNumberOfRequestsPerSession = filesStore.Conventions.MaxNumberOfRequestsPerSession;
            UseOptimisticConcurrency = filesStore.Conventions.DefaultUseOptimisticConcurrency;
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

        private Queue<IFilesOperation> registeredOperations = new Queue<IFilesOperation>();

        public void RegisterUpload(string path, Stream stream, RavenJObject metadata = null, Etag etag = null)
        {
            RegisterUploadInternal(new UploadFileOperation(this, path, stream, metadata, etag));
        }

        public void RegisterUpload(FileHeader file, Stream stream, Etag etag = null)
        {
            RegisterUploadInternal(new UploadFileOperation(this, file.FullPath, stream, file.Metadata, etag));
        }

        public void RegisterUpload(string path, long size, Action<Stream> write, RavenJObject metadata = null, Etag etag = null)
        {
            RegisterUploadInternal(new UploadFileOperation(this, path, size, write, metadata, etag));
        }

        public void RegisterUpload(FileHeader file, long size, Action<Stream> write, Etag etag = null)
        {
            RegisterUploadInternal(new UploadFileOperation(this, file.FullPath, size, write, file.Metadata, etag));
        }

        internal void RegisterUploadInternal(UploadFileOperation operation)
        {
            if (deletedEntities.Contains(operation.FileName))
                throw new InvalidOperationException("The file '" + operation.FileName + "' was already marked for deletion in this session, we do not allow delete and upload on the same session");

            FileHeader existingEntity;
            if (operation.Etag == null && UseOptimisticConcurrency && entitiesByKey.TryGetValue(operation.FileName, out existingEntity))
            {
                if (IsDeleted(operation.FileName) == false) // do not set etag if we already know that file was deleted
                    operation.Etag = existingEntity.Etag;
            }
            
            IncrementRequestCount();

            registeredOperations.Enqueue(operation);
        }

        public void RegisterFileDeletion(string path, Etag etag = null)
        {
            FileHeader existingEntity;
            if (etag == null && UseOptimisticConcurrency && entitiesByKey.TryGetValue(path, out existingEntity))
            {
                if (IsDeleted(path) == false) // do not set etag if we already know that file was deleted
                    etag = existingEntity.Etag;
            }

            deletedEntities.Add(path);

            var operation = new DeleteFileOperation(this, path, etag);

            IncrementRequestCount();

            registeredOperations.Enqueue(operation);
        }

        public void RegisterFileDeletion(FileHeader file, Etag etag = null)
        {
            RegisterFileDeletion(file.FullPath, etag);
        }

        public void RegisterDeletionQuery(string query)
        {
            var operation = new DeleteByQueryOperation(query);

            IncrementRequestCount();

            registeredOperations.Enqueue(operation);
        }

        public void RegisterRename(string sourceFile, string destinationFile, Etag etag = null)
        {
            FileHeader existingEntity;
            if (etag == null && UseOptimisticConcurrency && entitiesByKey.TryGetValue(sourceFile, out existingEntity))
            {
                if (IsDeleted(sourceFile) == false) // do not set etag if we already know that file was deleted
                    etag = existingEntity.Etag;
            }

            var operation = new RenameFileOperation(this, sourceFile, destinationFile, etag);

            IncrementRequestCount();

            registeredOperations.Enqueue(operation);
        }

        public void RegisterRename(FileHeader sourceFile, string destinationFile, Etag etag = null)
        {
            RegisterRename(sourceFile.FullPath, destinationFile, etag);
        }

        public void AddToCache(string filename, FileHeader fileHeader)
        {
            if (!entitiesByKey.ContainsKey(filename))
                entitiesByKey.Add(filename, fileHeader);
            else
                entitiesByKey[filename] = fileHeader;

            if (this.IsDeleted(filename))
                knownMissingIds.Remove(filename);
        }

        internal bool TryGetFromCache(string filename, out FileHeader fileHeader)
        {
            fileHeader = null;

            if (entitiesByKey.ContainsKey(filename))
                fileHeader = entitiesByKey[filename];

            return fileHeader != null;
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
            return knownMissingIds.Contains(id) || deletedEntities.Contains(id);
        }

        public bool DeletedInLastBatch(string id)
        {
            return deletedEntities.Contains(id);
        }

        public void RegisterMissing(string id)
        {
            knownMissingIds.Add(id);
        }

        public async Task SaveChangesAsync()
        {
            var changes = new SaveChangesData();
            var operationsToExecute = registeredOperations;
            registeredOperations = new Queue<IFilesOperation>();

            PrepareForSaveChanges(changes, operationsToExecute.ToList());

            try
            {
                var results = new List<FileHeader>();
                
                foreach (var op in changes.Operations)
                {
                    var operationResult = await op.Execute((IAsyncFilesSession)this).ConfigureAwait(false);
                    if (operationResult != null)
                        results.Add(operationResult);
                }

                ProcessResults(results, changes);
            }
            finally
            {
                deletedEntities.Clear();
            }
        }

        private void PrepareForSaveChanges(SaveChangesData changes, List<IFilesOperation> operations)
        {
            PrepareForDeletion(changes, operations);
            PrepareForUpdate(changes, operations);
            
            operations.RemoveAll(x => x.GetType() == typeof(DeleteFileOperation) || x.GetType() == typeof(DeleteByQueryOperation));

            // all other operations
            foreach (var op in operations)
            {
                changes.Entities.Add(op.FileName);
                changes.Operations.Add(op);
            }
        }

        private void PrepareForDeletion(SaveChangesData changes, List<IFilesOperation> operations)
        {
            var deleteOperations = operations.OfType<DeleteFileOperation>().ToList();
            foreach (var op in deleteOperations)
            {
                changes.Operations.Add(op);
                deletedEntities.Add(op.FileName);
            }

            var deleteByQueryOperations = operations.OfType<DeleteByQueryOperation>().ToList();
            foreach (var op in deleteByQueryOperations)
            {
                changes.Operations.Add(op);
            }
        }

        private void PrepareForUpdate(SaveChangesData changes, List<IFilesOperation> operations)
        {
            foreach(var key in entitiesByKey.Keys)
            {
                var fileHeader = entitiesByKey[key];

                if (EntityChanged(fileHeader) && !UploadRegisteredForFile(fileHeader.FullPath, operations))
                {
                    changes.Operations.Add(new UpdateMetadataOperation(this, fileHeader, fileHeader.Metadata, UseOptimisticConcurrency ? fileHeader.Etag : null));
                    changes.Entities.Add(fileHeader.FullPath);
                }
            }
        }

        private static bool UploadRegisteredForFile(string fileName, IEnumerable<IFilesOperation> operations)
        {
            return operations.Any(o => string.IsNullOrEmpty(o.FileName) == false && o.FileName == fileName && o.GetType() == typeof(UploadFileOperation));
        }

        private void ProcessResults(IList<FileHeader> results, SaveChangesData data)
        {
            for (var i = 0; i < results.Count; i++)
            {
                var result = results[i];
                var savedEntity = data.Entities[i];

                FileHeader existingEntity;
                if (entitiesByKey.TryGetValue(savedEntity, out existingEntity) == false)
                {
                    var operation = data.Operations[i];

                    if (operation is UploadFileOperation || operation is RenameFileOperation)
                    {
                        AddToCache(result.Name, result);
                    }
                    
                    continue;
                }

                var existingFileHeader = existingEntity;
                existingFileHeader.Metadata = result.Metadata;
                existingFileHeader.OriginalMetadata = (RavenJObject)result.Metadata.CloneToken();
                existingFileHeader.Refresh();

                if (savedEntity != result.FullPath)
                {
                    if (!entitiesByKey.ContainsKey(result.FullPath))
                    {
                        existingFileHeader.FullPath = result.FullPath;
                        entitiesByKey.Add(result.FullPath, existingFileHeader);
                        entitiesByKey.Remove(savedEntity);
                    }
                }

                AddToCache(existingFileHeader.FullPath, existingFileHeader);
            }
        }

        public bool EntityChanged(FileHeader fileHeader)
        {
            if (fileHeader == null)
                return true;

            return RavenJToken.DeepEquals(fileHeader.Metadata, fileHeader.OriginalMetadata, null) == false;
        }

        public bool EntityChanged(string filename)
        {
            if (filename == null || !entitiesByKey.ContainsKey(filename))
                return false;

            var fileHeader = entitiesByKey[filename];
            return EntityChanged(fileHeader);
        }

        public void IncrementRequestCount()
        {
            if (++NumberOfRequests > MaxNumberOfRequestsPerSession)
                throw new InvalidOperationException(
                    string.Format(
                        @"The maximum number of requests ({0}) allowed for this session has been reached.
RavenFS limits the number of remote calls that a session is allowed to make as an early warning system. Sessions are expected to be short lived, and 
RavenFS provides facilities like LoadFile(string[] path) to load multiple files at once.
You can increase the limit by setting FilesConvention.MaxNumberOfRequestsPerSession or MaxNumberOfRequestsPerSession, but it is
advisable that you'll look into reducing the number of remote calls first, since that will speed up your application significantly and result in a 
more responsive application.",
                        MaxNumberOfRequestsPerSession));
        }

        /// <summary>
        /// Data for a sending operations to the server
        /// </summary>
        internal class SaveChangesData
        {
            public SaveChangesData()
            {
                Operations = new List<IFilesOperation>();
                Entities = new List<String>();
            }

            /// <summary>
            /// Gets or sets the commands.
            /// </summary>
            /// <value>The commands.</value>
            internal List<IFilesOperation> Operations { get; set; }

            /// <summary>
            /// Gets or sets the entities.
            /// </summary>
            /// <value>The entities.</value>
            internal IList<String> Entities { get; set; }

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
