using System;
using Raven.Client.Documents.Operations;
using Sparrow.Json;

namespace Raven.Client.Documents.Session.Operations
{
    /// <summary>
    /// For internal session use only
    /// </summary>
    internal sealed class SessionOperationExecutor : OperationExecutor
    {
        private readonly InMemoryDocumentSessionOperations _session;

        public SessionOperationExecutor(InMemoryDocumentSessionOperations session)
            : base(session.DocumentStore, session.DatabaseName)
        {
            _session = session;
        }

        public override OperationExecutor ForDatabase(string databaseName)
        {
            throw new NotSupportedException("This method is not supported");
        }

        protected override IDisposable GetContext(out JsonOperationContext context)
        {
            context = _session.Context;
            return null;
        }
    }
}
