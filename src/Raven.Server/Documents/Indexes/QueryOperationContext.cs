using System;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public sealed class QueryOperationContext : IDisposable
    {
        private readonly DocumentDatabase _database;
        public readonly DocumentsOperationContext Documents;

        public ClusterOperationContext Server;

        private IDisposable _releaseDocuments;
        private IDisposable _releaseServer;

        private QueryOperationContext(DocumentDatabase database, DocumentsOperationContext documentsContext, bool releaseDocumentsContext, bool needsServerContext)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            Documents = documentsContext ?? throw new ArgumentNullException(nameof(documentsContext));

            if (releaseDocumentsContext)
                _releaseDocuments = Documents;

            if (needsServerContext)
                _releaseServer = database.ServerStore.Engine.ContextPool.AllocateOperationContext(out Server);
        }

        private QueryOperationContext(DocumentDatabase database, bool needsServerContext)
        {
            _database = database ?? throw new ArgumentNullException(nameof(database));
            _releaseDocuments = database.DocumentsStorage.ContextPool.AllocateOperationContext(out Documents);

            if (needsServerContext)
                _releaseServer = database.ServerStore.Engine.ContextPool.AllocateOperationContext(out Server);
        }

        internal void WithIndex(Index index)
        {
            if (Server != null)
                return;

            if (index.Definition.HasCompareExchange)
                _releaseServer = _database.ServerStore.Engine.ContextPool.AllocateOperationContext(out Server);
        }

        internal void WithQuery(QueryMetadata metadata)
        {
            if (Server != null)
                return;

            if (metadata.HasCmpXchg || metadata.HasCmpXchgSelect || metadata.HasCmpXchgIncludes)
                _releaseServer = _database.ServerStore.Engine.ContextPool.AllocateOperationContext(out Server);
        }

        public IDisposable OpenReadTransaction([CallerMemberName] string caller = null)
        {
            var documentsTx = Documents.OpenReadTransaction(caller);
            RavenTransaction serverTx = null;
            if (Server != null)
                serverTx = Server.OpenReadTransaction(caller);

            return new DisposeTransactions(documentsTx, serverTx);
        }

        public void CloseTransaction()
        {
            Documents.CloseTransaction();
            Server?.CloseTransaction();
        }

        public bool AreTransactionsOpened()
        {
            var opened = Documents.Transaction?.Disposed == false;
            if (Server == null)
                return opened;

            var serverOpened = Server.Transaction?.Disposed == false;
            if (opened != serverOpened)
                ThrowTransactionsNotInTheSameStateException(opened, serverOpened);

            return opened;
        }

        public void SetLongLivedTransactions(bool value)
        {
            Documents.PersistentContext.LongLivedTransactions = value;

            if (Server != null)
                Server.PersistentContext.LongLivedTransactions = value;
        }

        [Conditional("DEBUG")]
        public void AssertOpenedTransactions()
        {
            if (Documents.Transaction == null)
                throw new InvalidOperationException("Expected documents transaction to be opened.");

            if (Server != null && Server.Transaction == null)
                throw new InvalidOperationException("Expected server transaction to be opened.");
        }

        public void Dispose()
        {
            _releaseDocuments?.Dispose();
            _releaseDocuments = null;

            _releaseServer?.Dispose();
            _releaseServer = null;
        }

        public static QueryOperationContext Allocate(DocumentDatabase database, bool needsServerContext = false)
        {
            return new QueryOperationContext(database, needsServerContext);
        }

        public static QueryOperationContext Allocate(DocumentDatabase database, Index index)
        {
            var queryContext = Allocate(database);
            queryContext.WithIndex(index);

            return queryContext;
        }

        /// <summary>
        /// For testing purposes only
        /// </summary>
        public static QueryOperationContext ShortTermSingleUse(DocumentDatabase database)
        {
            var documentsContext = DocumentsOperationContext.ShortTermSingleUse(database);

            return new QueryOperationContext(database, documentsContext, releaseDocumentsContext: true, needsServerContext: false);
        }

        private struct DisposeTransactions : IDisposable
        {
            private DocumentsTransaction _documentsTx;
            private RavenTransaction _serverTx;

            public DisposeTransactions(DocumentsTransaction documentsTx, RavenTransaction serverTx)
            {
                _documentsTx = documentsTx;
                _serverTx = serverTx;
            }

            public void Dispose()
            {
                _documentsTx?.Dispose();
                _documentsTx = null;

                _serverTx?.Dispose();
                _serverTx = null;
            }
        }

        [DoesNotReturn]
        private static void ThrowTransactionsNotInTheSameStateException(bool opened, bool serverOpened)
        {
            throw new InvalidOperationException($"Documents transaction ('{opened}') and server transaction ('{serverOpened}') do not have the same state.");
        }
    }
}
