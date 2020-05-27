using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public abstract class AdvancedSessionExtensionBase
    {
        protected AdvancedSessionExtensionBase(InMemoryDocumentSessionOperations session)
        {
            Session = session;
            RequestExecutor = Session.RequestExecutor;
            SessionInfo = Session._sessionInfo;
            Context = Session.Context;
            DocumentStore = Session.DocumentStore;
            DeferredCommandsDictionary = Session.DeferredCommandsDictionary;
            DocumentsById = Session.DocumentsById;
        }

        protected InMemoryDocumentSessionOperations Session { get; }
        protected RequestExecutor RequestExecutor { get; }
        protected SessionInfo SessionInfo { get; }
        protected JsonOperationContext Context { get; }
        protected IDocumentStore DocumentStore { get; }
        protected Dictionary<(string, CommandType, string), ICommandData> DeferredCommandsDictionary { get; }
        internal DocumentsById DocumentsById { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Defer(ICommandData command, params ICommandData[] commands)
        {
            Session.Defer(command, commands);
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
}
