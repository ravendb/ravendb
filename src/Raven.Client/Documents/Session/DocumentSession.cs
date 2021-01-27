//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Commands.MultiGet;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, IAdvancedSessionOperations, IDocumentSessionImpl
    {
        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        public IAdvancedSessionOperations Advanced => this;

        /// <summary>
        /// Access the eager operations
        /// </summary>
        public IEagerSessionOperations Eagerly => this;

        /// <summary>
        /// Access the lazy operations
        /// </summary>
        public ILazySessionOperations Lazily => this;

        /// <summary>
        /// Access the attachments operations
        /// </summary>
        public IAttachmentsSessionOperations Attachments => _attachments ?? (_attachments = new DocumentSessionAttachments(this));
        private IAttachmentsSessionOperations _attachments;

        /// <summary>        
        /// Access the revisions operations
        /// </summary>
        public IRevisionsSessionOperations Revisions => _revisions ?? (_revisions = new DocumentSessionRevisions(this));
        private IRevisionsSessionOperations _revisions;

        /// <summary>
        /// Access to cluster wide transaction operations
        /// </summary>
        public IClusterTransactionOperations ClusterTransaction => _clusterTransaction ?? (_clusterTransaction = new ClusterTransactionOperations(this));
        private IClusterTransactionOperations _clusterTransaction;

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
                _clusterTransaction = new ClusterTransactionOperations(this);

            return (ClusterTransactionOperationsBase)_clusterTransaction;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentSession"/> class.
        /// </summary>
        public DocumentSession(DocumentStore documentStore, Guid id, SessionOptions options)
            : base(documentStore, id, options)
        {
        }

        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        public void SaveChanges()
        {
            var saveChangesOperation = new BatchOperation(this);

            using (var command = saveChangesOperation.CreateRequest())
            {
                if (command == null)
                    return;

                if (NoTracking)
                    throw new InvalidOperationException($"Cannot execute '{nameof(SaveChanges)}' when entity tracking is disabled in session.");

                RequestExecutor.Execute(command, Context, sessionInfo: _sessionInfo);
                UpdateSessionAfterSaveChanges(command.Result);
                saveChangesOperation.SetResult(command.Result);
            }
        }

        /// <summary>
        /// Check if document exists without loading it
        /// </summary>
        /// <param name="id">Document id.</param>
        /// <returns></returns>
        public bool Exists(string id)
        {
            if (id == null)
                throw new ArgumentNullException(nameof(id));

            if (_knownMissingIds.Contains(id))
                return false;

            if (DocumentsById.TryGetValue(id, out _))
                return true;

            var command = new HeadDocumentCommand(id, null);
            RequestExecutor.Execute(command, Context, sessionInfo: _sessionInfo);

            return command.Result != null;
        }

        /// <summary>
        /// Refreshes the specified entity from Raven server.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="entity">The entity.</param>
        public void Refresh<T>(T entity)
        {
            DocumentInfo documentInfo;
            if (DocumentsByEntity.TryGetValue(entity, out documentInfo) == false)
                throw new InvalidOperationException("Cannot refresh a transient instance");
            IncrementRequestCount();

            var command = new GetDocumentsCommand(new[] { documentInfo.Id }, includes: null, metadataOnly: false);
            RequestExecutor.Execute(command, Context, sessionInfo: _sessionInfo);

            RefreshInternal(entity, command, documentInfo);
        }

        /// <summary>
        /// Generates the document ID.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        protected override string GenerateId(object entity)
        {
            return Conventions.GenerateDocumentId(DatabaseName, entity);
        }

        /// <summary>
        /// Not supported on sync session.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        protected override Task<string> GenerateIdAsync(object entity)
        {
            throw new NotSupportedException("Cannot use async operation in sync session");
        }

        public ResponseTimeInformation ExecuteAllPendingLazyOperations()
        {
            var requests = new List<GetRequest>();
            for (int i = 0; i < PendingLazyOperations.Count; i++)
            {
                var req = PendingLazyOperations[i].CreateRequest(Context);
                if (req == null)
                {
                    PendingLazyOperations.RemoveAt(i);
                    i--; // so we'll recheck this index
                    continue;
                }
                requests.Add(req);
            }

            if (requests.Count == 0)
                return new ResponseTimeInformation();

            try
            {
                var sw = Stopwatch.StartNew();

                var responseTimeDuration = new ResponseTimeInformation();

                while (ExecuteLazyOperationsSingleStep(responseTimeDuration, requests, sw))
                {
                    Thread.Sleep(100);
                }

                responseTimeDuration.ComputeServerTotal();


                foreach (var pendingLazyOperation in PendingLazyOperations)
                {
                    Action<object> value;
                    if (OnEvaluateLazy.TryGetValue(pendingLazyOperation, out value))
                        value(pendingLazyOperation.Result);
                }
                responseTimeDuration.TotalClientDuration = sw.Elapsed;
                return responseTimeDuration;
            }
            finally
            {
                PendingLazyOperations.Clear();
            }
        }

        private bool ExecuteLazyOperationsSingleStep(ResponseTimeInformation responseTimeInformation, List<GetRequest> requests, Stopwatch sw)
        {
            var multiGetOperation = new MultiGetOperation(this);
            var multiGetCommand = multiGetOperation.CreateRequest(requests);
            RequestExecutor.Execute(multiGetCommand, Context, sessionInfo: _sessionInfo);
            var responses = multiGetCommand.Result;
            if(multiGetCommand.AggressivelyCached == false)
                IncrementRequestCount();

            for (var i = 0; i < PendingLazyOperations.Count; i++)
            {
                long totalTime;
                string tempReqTime;
                var response = responses[i];

                response.Headers.TryGetValue(Constants.Headers.RequestTime, out tempReqTime);
                response.Elapsed = sw.Elapsed;

                long.TryParse(tempReqTime, out totalTime);

                responseTimeInformation.DurationBreakdown.Add(new ResponseTimeItem
                {
                    Url = requests[i].UrlAndQuery,
                    Duration = TimeSpan.FromMilliseconds(totalTime)
                });

                if (response.RequestHasErrors())
                    throw new InvalidOperationException("Got an error from server, status code: " + (int)response.StatusCode + Environment.NewLine + response.Result);

                PendingLazyOperations[i].HandleResponse(response);
                if (PendingLazyOperations[i].RequiresRetry)
                {
                    return true;
                }
            }
            return false;
        }
    }
}
