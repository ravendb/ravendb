//-----------------------------------------------------------------------
// <copyright file="AsyncDocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Extensions;
using Raven.Client.Http;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implementation for async document session 
    /// </summary>
    public partial class AsyncDocumentSession : InMemoryDocumentSessionOperations, IAsyncDocumentSessionImpl, IAsyncAdvancedSessionOperations, IDocumentQueryGenerator
    {
        private AsyncDocumentIdGeneration _asyncDocumentIdGeneration;
        private OperationExecutor _operations;

        /// <summary>
        /// Initializes a new instance of the <see cref="AsyncDocumentSession"/> class.
        /// </summary>
        public AsyncDocumentSession(string dbName, DocumentStore documentStore, RequestExecutor requestExecutor, Guid id)
            : base(dbName, documentStore, requestExecutor, id)
        {
            GenerateDocumentIdsOnStore = false;
        }

        public string GetDocumentUrl(object entity)
        {
            DocumentInfo document;
            if (DocumentsByEntity.TryGetValue(entity, out document) == false)
                throw new InvalidOperationException("Could not figure out identifier for transient instance");

            return RequestExecutor.UrlFor(document.Id);
        }

        public async Task<bool> ExistsAsync(string id)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (DocumentsById.TryGetValue(id, out _))
                return true;

            var command = new HeadDocumentCommand(id, null);
            await RequestExecutor.ExecuteAsync(command, Context);

            return command.Result != null;
        }

        public async Task RefreshAsync<T>(T entity, CancellationToken token = default(CancellationToken))
        {
            DocumentInfo documentInfo;
            if (DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                throw new InvalidOperationException("Cannot refresh a transient instance");
            IncrementRequestCount();

            var command = new GetDocumentCommand(new[] { documentInfo.Id }, includes: null, transformer: null, transformerParameters: null, metadataOnly: false, context: Context);
            await RequestExecutor.ExecuteAsync(command, Context, token);

            RefreshInternal(entity, command, documentInfo);
        }

        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate
        /// property to avoid cluttering the API
        /// </remarks>
        public IAsyncAdvancedSessionOperations Advanced => this;

        protected override string GenerateId(object entity)
        {
            throw new NotSupportedException("Async session cannot generate IDs synchronously");
        }

        protected override void RememberEntityForDocumentIdGeneration(object entity)
        {
            EnsureAsyncDocumentIdGeneration();
            _asyncDocumentIdGeneration.Add(entity);
        }

        private void EnsureAsyncDocumentIdGeneration()
        {
            if (_asyncDocumentIdGeneration != null) return;
            _asyncDocumentIdGeneration = new AsyncDocumentIdGeneration(this, DocumentsByEntity.TryGetValue, (id, entity, metadata) => id);
        }

        protected override Task<string> GenerateIdAsync(object entity)
        {
            return Conventions.GenerateDocumentIdAsync(DatabaseName, entity);
        }

        public IAsyncEagerSessionOperations Eagerly => this;

        public IAsyncLazySessionOperations Lazily => this;

        /// <summary>
        /// Begins the async save changes operation
        /// </summary>
        /// <returns></returns>
        public async Task SaveChangesAsync(CancellationToken token = default(CancellationToken))
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

                await RequestExecutor.ExecuteAsync(command, Context, token);
                saveChangesOperation.SetResult(command.Result);
            }
        }


    }
}
