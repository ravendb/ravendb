using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    public class AdvancedSessionExtentionBase
    {
        protected AdvancedSessionExtentionBase(InMemoryDocumentSessionOperations session)
        {
            Session = session;
            DocumentsByEntity = Session.DocumentsByEntity;
            RequestExecutor = Session.RequestExecutor;
            SessionInfo = Session.SessionInfo;
            Context = Session.Context;
            DocumentStore = Session.DocumentStore;
            DeferredCommandsDictionary = Session.DeferredCommandsDictionary;
            DeletedEntities = Session.DeletedEntities;
            DocumentsById = Session.DocumentsById;
        }

        protected InMemoryDocumentSessionOperations Session { get; }
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
            Session.Defer(command, commands);
        }
    }
}
