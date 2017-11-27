using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class AdvancedSessionExtentionBase
    {
        private readonly InMemoryDocumentSessionOperations _session;

        protected AdvancedSessionExtentionBase(InMemoryDocumentSessionOperations session)
        {
            _session = session;
            DocumentsByEntity = _session.DocumentsByEntity;
            RequestExecutor = _session.RequestExecutor;
            SessionInfo = _session.SessionInfo;
            Context = _session.Context;
            DocumentStore = _session.DocumentStore;
            DeferredCommandsDictionary = _session.DeferredCommandsDictionary;
            DeletedEntities = _session.DeletedEntities;
            DocumentsById = _session.DocumentsById;
        }

        protected Dictionary<object, DocumentInfo> DocumentsByEntity { get; }
        protected RequestExecutor RequestExecutor { get; }
        protected SessionInfo SessionInfo { get; }
        protected JsonOperationContext Context { get; }
        protected IDocumentStore DocumentStore { get; }
        protected Dictionary<(string, CommandType, string), ICommandData> DeferredCommandsDictionary { get; }
        protected HashSet<object> DeletedEntities { get; }
        internal DocumentsById DocumentsById { get; }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Defer(ICommandData command, params ICommandData[] commands)
        {
            _session.Defer(command, commands);
        }
    }
}
