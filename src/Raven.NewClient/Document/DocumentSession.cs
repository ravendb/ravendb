//-----------------------------------------------------------------------
// <copyright file="DocumentSession.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.NewClient.Client.Connection;
using System;
using System.Diagnostics;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;
using Raven.NewClient.Client.Commands;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Data.Queries;
using Raven.NewClient.Client.Document.Batches;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Linq;
using System.Linq;
using Raven.NewClient.Abstractions.Data;
using Sparrow.Json;

namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public partial class DocumentSession : InMemoryDocumentSessionOperations, IDocumentQueryGenerator, ISyncAdvancedSessionOperation, IDocumentSessionImpl
    {
        /// <summary>
        /// Get the accessor for advanced operations
        /// </summary>
        /// <remarks>
        /// Those operations are rarely needed, and have been moved to a separate 
        /// property to avoid cluttering the API
        /// </remarks>
        public ISyncAdvancedSessionOperation Advanced => this;

        /// <summary>
        /// Access the lazy operations
        /// </summary>
        public ILazySessionOperations Lazily => this;

        /// <summary>
        /// Access the eager operations
        /// </summary>
        public IEagerSessionOperations Eagerly => this;

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentSession"/> class.
        /// </summary>
        public DocumentSession(string dbName, DocumentStore documentStore, Guid id, RequestExecuter requestExecuter)
            : base(dbName, documentStore, requestExecuter, id)
        {
            
        }
        
        #region DeleteByIndex

        public Operation DeleteByIndex<T, TIndexCreator>(Expression<Func<T, bool>> expression) where TIndexCreator : AbstractIndexCreationTask, new()
        {
            var indexCreator = new TIndexCreator();
            return DeleteByIndex<T>(indexCreator.IndexName, expression);
        }

        public Operation DeleteByIndex<T>(string indexName, Expression<Func<T, bool>> expression)
        {
            var query = Query<T>(indexName).Where(expression);
            var indexQuery = new IndexQuery()
            {
                Query = query.ToString()
            };

            var deleteByIndexOperation = new DeleteByIndexOperation();
            var command = deleteByIndexOperation.CreateRequest(indexName, indexQuery,
                new QueryOperationOptions(), (DocumentStore)this.DocumentStore);

            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                return new Operation(command.Result.OperationId);
            }
            return null;
        }

        #endregion
        /// <summary>
        /// Saves all the changes to the Raven server.
        /// </summary>
        public void SaveChanges()
        {
            var saveChangesOperation = new BatchOperation(this);

            var command = saveChangesOperation.CreateRequest();
            if (command != null)
            {
                RequestExecuter.Execute(command, Context);
                saveChangesOperation.SetResult(command.Result);
            }
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

            var command = new GetDocumentCommand
            {
                Ids = new[] { documentInfo.Id },
                Context = this.Context
            };
            RequestExecuter.Execute(command, Context);

            RefreshInternal(entity, command, documentInfo);
        }

        /// <summary>
        /// Gets the document URL for the specified entity.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        public string GetDocumentUrl(object entity)
        {
            DocumentInfo document;
            if (DocumentsByEntity.TryGetValue(entity, out document) == false)
                throw new InvalidOperationException("Could not figure out identifier for transient instance");

            return RequestExecuter.UrlFor(document.Id);
        }
         
        public FacetedQueryResult[] MultiFacetedSearch(params FacetQuery[] queries)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Generates the document key.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        protected override string GenerateKey(object entity)
        {
            return Conventions.GenerateDocumentKey(DatabaseName, entity);
        }

        /// <summary>
        /// Not supported on sync session.
        /// </summary>
        /// <param name="entity">The entity.</param>
        /// <returns></returns>
        protected override Task<string> GenerateKeyAsync(object entity)
        {
            throw new NotSupportedException("Cannot use async operation in sync session");
        }

        public ResponseTimeInformation ExecuteAllPendingLazyOperations()
        {
            if (pendingLazyOperations.Count == 0)
                return new ResponseTimeInformation();

            try
            {
                var sw = Stopwatch.StartNew();

                IncrementRequestCount();

                var responseTimeDuration = new ResponseTimeInformation();

                while (ExecuteLazyOperationsSingleStep(responseTimeDuration))
                {
                    Thread.Sleep(100);
                }

                responseTimeDuration.ComputeServerTotal();


                foreach (var pendingLazyOperation in pendingLazyOperations)
                {
                    Action<object> value;
                    if (onEvaluateLazy.TryGetValue(pendingLazyOperation, out value))
                        value(pendingLazyOperation.Result);
                }
                responseTimeDuration.TotalClientDuration = sw.Elapsed;
                return responseTimeDuration;
            }
            finally
            {
                pendingLazyOperations.Clear();
            }
        }

        private bool ExecuteLazyOperationsSingleStep(ResponseTimeInformation responseTimeInformation)
        {
            //WIP - Not final
            var requests = pendingLazyOperations.Select(x => x.CreateRequest()).ToList();
            var multiGetOperation = new MultiGetOperation(this);
            var multiGetCommand = multiGetOperation.CreateRequest(requests);
            RequestExecuter.Execute(multiGetCommand, Context);
            var responses = multiGetCommand.Result;

            for ( var i = 0; i < pendingLazyOperations.Count; i++)
            {
                long totalTime;
                string tempReqTime;
                var response = (BlittableJsonReaderObject)responses.Results[i];
                BlittableJsonReaderObject headers;
                response.TryGet("Headers", out headers);
                headers.TryGet(Constants.Headers.RequestTime, out tempReqTime);

                long.TryParse(tempReqTime, out totalTime);

                responseTimeInformation.DurationBreakdown.Add(new ResponseTimeItem
                {
                    Url = requests[i].UrlAndQuery,
                    Duration = TimeSpan.FromMilliseconds(totalTime)
                });

                long status;
                response.TryGet("Status", out status);
                switch (status)
                {
                    case 0:   // aggressively cached
                    case 200: // known non error values
                    case 201:
                    case 203:
                    case 204:
                    case 304:
                    case 404:
                        break;
                    default:
                        throw new InvalidOperationException("Got an error from server, status code: " + (int)status +
                                                        Environment.NewLine + response);
                }

                pendingLazyOperations[i].HandleResponse(response);
                if (pendingLazyOperations[i].RequiresRetry)
                {
                    return true;
                }
            }
            return false;
        }
    }
}