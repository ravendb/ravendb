using System.Collections.Generic;
using System.Runtime.CompilerServices;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Http;
using Sparrow.Json;

namespace Raven.Client.Documents.Session
{
    /// <summary>
    /// Abstract base class for advanced session extensions
    /// </summary>
    public abstract class AdvancedSessionExtensionBase
    {
        /// <summary>
        /// Initializes a new instance of the AdvancedSessionExtensionBase class
        /// </summary>
        /// <param name="session">The in-memory document session operations</param>
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

        /// <summary>
        /// Defer commands to be executed on SaveChanges()
        /// </summary>
        /// <param name="command">Command to be executed</param>
        /// <param name="commands">Array of commands to be executed</param>
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public void Defer(ICommandData command, params ICommandData[] commands)
        {
            Session.Defer(command, commands);
        }
    }
}
