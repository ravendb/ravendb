//-----------------------------------------------------------------------
// <copyright file="DocumentSessionRevisions.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Documents.Session.Operations.Lazy;
using Raven.Client.Json;

namespace Raven.Client.Documents.Session
{
    public abstract class DocumentSessionRevisionsBase : AdvancedSessionExtensionBase
    {

        protected DocumentSessionRevisionsBase(InMemoryDocumentSessionOperations session)
            : base(session)
        {
        }
        public void ForceRevisionCreationFor<T>(T entity, ForceRevisionStrategy strategy = ForceRevisionStrategy.Before)
        {
            if (ReferenceEquals(entity, null))
                throw new ArgumentNullException(nameof(entity));

            if (Session.DocumentsByEntity.TryGetValue(entity, out DocumentInfo documentInfo) == false)
            {
                throw new InvalidOperationException("Cannot create a revision for the requested entity because it is Not tracked by the session");
            }

            AddIdToList(documentInfo.Id, strategy);
        }

        public void ForceRevisionCreationFor(string id, ForceRevisionStrategy strategy = ForceRevisionStrategy.Before)
        {
            AddIdToList(id, strategy);
        }

        private void AddIdToList(string id, ForceRevisionStrategy requestedStrategy)
        {
            if (String.IsNullOrEmpty(id))
            {
                throw new InvalidOperationException("Id cannot be null or empty.");
            }

            var idAlreadyAdded = Session.IdsForCreatingForcedRevisions.TryGetValue(id, out var existingStrategy);
            if (idAlreadyAdded && (existingStrategy != requestedStrategy))
            {
                throw new InvalidOperationException($"A request for creating a revision was already made for document {id} in the current session but with a different force strategy." +
                                                    $"New strategy requested: {requestedStrategy}. Previous strategy: {existingStrategy}.");
            }

            if (idAlreadyAdded == false)
            {
                Session.IdsForCreatingForcedRevisions.Add(id, requestedStrategy);
            }
        }
    }

    /// <summary>
    /// Implements Unit of Work for accessing the RavenDB server
    /// </summary>
    public class DocumentSessionRevisions : DocumentSessionRevisionsBase, IRevisionsSessionOperations,ILazyRevisionsOperations
    {
        public DocumentSessionRevisions(InMemoryDocumentSessionOperations session) : base(session)
        {
        }

        public ILazyRevisionsOperations Lazily => this;

        public List<T> GetFor<T>(string id, int start = 0, int pageSize = 25)
        {
            var operation = new GetRevisionOperation(Session, id, start, pageSize);

            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevisionsFor<T>();
        }
        
        public List<MetadataAsDictionary> GetMetadataFor(string id, int start = 0, int pageSize = 25)
        {
            var operation = new GetRevisionOperation(Session, id, start, pageSize, true);
            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevisionsMetadataFor();
        }

        public T Get<T>(string changeVector)
        {
            var operation = new GetRevisionOperation(Session, changeVector);

            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevision<T>();
        }

        public Dictionary<string, T> Get<T>(IEnumerable<string> changeVectors)
        {
            var operation = new GetRevisionOperation(Session, changeVectors);

            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevisions<T>();
        }

        public T Get<T>(string id, DateTime date)
        {
            var operation = new GetRevisionOperation(Session, id, date);
            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            operation.SetResult(command.Result);
            return operation.GetRevisionsFor<T>().FirstOrDefault();
        }

        public long GetCountFor(string id)
        {
            var operation = new GetRevisionsCountOperation(id);
            var command = operation.CreateRequest();
            RequestExecutor.Execute(command, Context, sessionInfo: SessionInfo);
            return command.Result;
        }
        Lazy<T> ILazyRevisionsOperations.Get<T>(string changeVector)
        {
            var operation = new GetRevisionOperation(Session, changeVector);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Single);
            return ((DocumentSession)Session).AddLazyOperation<T>(lazyRevisionOperation, null);
        }

        Lazy<List<MetadataAsDictionary>> ILazyRevisionsOperations.GetMetadataFor(string id, int start, int pageSize)
        {
            var operation = new GetRevisionOperation(Session, id, start, pageSize);
            var lazyRevisionOperation = new LazyRevisionOperation<MetadataAsDictionary>(operation, LazyRevisionOperation<MetadataAsDictionary>.Mode.ListOfMetadata);
            return ((DocumentSession)Session).AddLazyOperation<List<MetadataAsDictionary>>(lazyRevisionOperation, null);
        }

        Lazy<Dictionary<string, T>> ILazyRevisionsOperations.Get<T>(IEnumerable<string> changeVectors)
        {
            var operation = new GetRevisionOperation(Session,changeVectors);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Map);
            return ((DocumentSession)Session).AddLazyOperation<Dictionary<string, T>>(lazyRevisionOperation, null);
        }

        Lazy<T> ILazyRevisionsOperations.Get<T>(string id, DateTime date)
        {
            var operation = new GetRevisionOperation(Session, id, date);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Single);
            return ((DocumentSession)Session).AddLazyOperation<T>(lazyRevisionOperation, null);
        }
        
        Lazy<List<T>> ILazyRevisionsOperations.GetFor<T>(string id, int start, int pageSize)
        {
            var operation = new GetRevisionOperation(Session,id,start, pageSize);
            var lazyRevisionOperation = new LazyRevisionOperation<T>(operation, LazyRevisionOperation<T>.Mode.Multi);
            return ((DocumentSession)Session).AddLazyOperation<List<T>>(lazyRevisionOperation, null);
        }

  
    }
}
