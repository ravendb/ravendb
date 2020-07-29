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
    }
}
