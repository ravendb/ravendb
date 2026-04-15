using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Extensions;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implementation for async document session
    /// </summary>
    public partial class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
    {
        private AsyncDocumentIdGeneration _asyncDocumentIdGeneration;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
        /// </summary>
        public AsyncDocumentSession(DocumentStore documentStore, Guid id, SessionOptions options)
            : base(documentStore, id, options)
        {
            GenerateDocumentIdsOnStore = false;
        }

        /// <summary>
        /// Check if document exists without loading it asynchronously
        /// </summary>
        /// <param name="id">Document id</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains true if the document exists; otherwise, false</returns>
        public async Task<bool> ExistsAsync(string id, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                if (id == null)
                    throw new ArgumentNullException(nameof(id));

                if (_knownMissingIds.Contains(id))
                    return false;

                if (DocumentsById.TryGetValue(id, out _))
                    return true;

                var command = new HeadDocumentCommand(id, null);
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: _sessionInfo, token: token).ConfigureAwait(false);

                return command.Result != null;
            }
        }

        /// <summary>
        /// Refreshes the specified entity from Raven server asynchronously
        /// </summary>
        /// <typeparam name="T">The type of the entity</typeparam>
        /// <param name="entity">The entity to refresh</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous refresh operation</returns>
        public async Task RefreshAsync<T>(T entity, CancellationToken token = default(CancellationToken))
        {
            using (AsyncTaskHolder())
            {
                if (DocumentsByEntity.TryGetValue(entity, out var documentInfo) == false)
                    throw new InvalidOperationException("Cannot refresh a transient instance");
                IncrementRequestCount();

                var command = new GetDocumentsCommand(Conventions, new[] { documentInfo.Id }, includes: null, metadataOnly: false);
                await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);

                var commandResult = (BlittableJsonReaderObject)command.Result.Results[0];
                RefreshInternal(entity, commandResult, documentInfo);
            }
        }

        /// <summary>
        /// Refreshes the specified entities from Raven server asynchronously
        /// </summary>
        /// <typeparam name="T">The type of the entities</typeparam>
        /// <param name="entities">The entities to refresh</param>
        /// <param name="token">The cancellation token</param>
        /// <returns>A task that represents the asynchronous refresh operation</returns>
        public async Task RefreshAsync<T>(IEnumerable<T> entities, CancellationToken token = default)
        {
            using (AsyncTaskHolder())
            {
                BuildEntityDocInfoByIdHolder(entities, out var idsEntitiesPairs);

                IncrementRequestCount();

                var command = new GetDocumentsCommand(Conventions, idsEntitiesPairs.Keys.ToArray(), includes: null, metadataOnly: false);
                await RequestExecutor.ExecuteAsync(command, Context, sessionInfo: _sessionInfo, token).ConfigureAwait(false);

                RefreshEntities(command, idsEntitiesPairs);
            }
        }

        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate
        /// property to avoid cluttering the API
        /// </remarks>
        public IAsyncAdvancedSessionOperations Advanced => this;

        protected override void RememberEntityForDocumentIdGeneration(object entity)
        {
            EnsureAsyncDocumentIdGeneration();
            _asyncDocumentIdGeneration.Add(entity);
        }

        private void EnsureAsyncDocumentIdGeneration()
        {
            if (_asyncDocumentIdGeneration != null)
                return;
            _asyncDocumentIdGeneration = new AsyncDocumentIdGeneration(this, DocumentsByEntity.TryGetValue, (id, entity, metadata) => id);
        }

        protected override Task<string> GenerateIdAsync(object entity)
        {
            return Conventions.GenerateDocumentIdAsync(DatabaseName, entity);
        }

        /// <summary>
        /// Access the eager session operations for loading entities immediately
        /// </summary>
        public IAsyncEagerSessionOperations Eagerly => this;

        /// <summary>
        /// Access the lazy session operations for deferred loading of entities
        /// </summary>
        public IAsyncLazySessionOperations Lazily => this;

        /// <summary>
        /// Access the async attachments operations for this session
        /// </summary>
        public IAttachmentsSessionOperationsAsync Attachments => _attachments ?? (_attachments = new DocumentSessionAttachmentsAsync(this));
        private IAttachmentsSessionOperationsAsync _attachments;

        /// <summary>
        /// Access the async revisions operations for this session
        /// </summary>
        public IRevisionsSessionOperationsAsync Revisions => _revisions ?? (_revisions = new DocumentSessionRevisionsAsync(this));
        private IRevisionsSessionOperationsAsync _revisions;

        /// <summary>
        /// Access the async cluster-wide transaction operations for this session
        /// </summary>
        public IClusterTransactionOperationsAsync ClusterTransaction => _clusterTransaction ?? (_clusterTransaction = new ClusterTransactionOperationsAsync(this));
        private IClusterTransactionOperationsAsync _clusterTransaction;

        protected override bool HasClusterSession => _clusterTransaction != null;

        protected override void ClearClusterSession()
        {
            if (HasClusterSession == false)
                return;

            GetClusterSession().Clear();
        }

        protected internal override ClusterTransactionOperationsBase GetClusterSession()
        {
            if (_clusterTransaction == null)
                _clusterTransaction = new ClusterTransactionOperationsAsync(this);

            return (ClusterTransactionOperationsBase)_clusterTransaction;
        }

        /// <summary>
        /// Begins the async save changes operation
        /// </summary>
        /// <returns></returns>
        public async Task SaveChangesAsync(CancellationToken token = default(CancellationToken))
        {
            AssertNotDisposed();
            using (AsyncTaskHolder())
            {
                if (_asyncDocumentIdGeneration != null)
                {
                    await _asyncDocumentIdGeneration.GenerateDocumentIdsForSaveChanges().WithCancellation(token).ConfigureAwait(false);
                }

                var saveChangesOperation = new BatchOperation(this);

                using (var command = saveChangesOperation.CreateRequest())
                {
                    if (command == null)
                        return;

                    if (NoTracking)
                        throw new InvalidOperationException($"Cannot execute '{nameof(SaveChangesAsync)}' when entity tracking is disabled in session.");

                    await RequestExecutor.ExecuteAsync(command, Context, _sessionInfo, token).ConfigureAwait(false);
                    UpdateSessionAfterSaveChanges(command.Result);
                    saveChangesOperation.SetResult(command.Result);
                }
            }
        }
    }
}