using System;
using System.Diagnostics;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.Indexes
{
    public class QueryOperationContext : IDisposable
    {
        public readonly DocumentsOperationContext Documents;

        public readonly TransactionOperationContext Server;

        private IDisposable _releaseDocuments;
        private IDisposable _releaseServer;

        private QueryOperationContext(DocumentsOperationContext documentsContext, bool releaseDocumentsContext, bool needsServerContext)
        {
            if (documentsContext is null)
                throw new ArgumentNullException(nameof(documentsContext));

            Documents = documentsContext;

            if (releaseDocumentsContext)
                _releaseDocuments = Documents;

            if (needsServerContext)
                _releaseServer = documentsContext.DocumentDatabase.ServerStore.ContextPool.AllocateOperationContext(out Server);
        }

        private QueryOperationContext(DocumentDatabase database, bool needsServerContext)
        {
            if (database is null)
                throw new ArgumentNullException(nameof(database));

            _releaseDocuments = database.DocumentsStorage.ContextPool.AllocateOperationContext(out Documents);

            if (needsServerContext)
                _releaseServer = database.ServerStore.ContextPool.AllocateOperationContext(out Server);
        }

        public IDisposable OpenReadTransaction()
        {
            var documentsTx = Documents.OpenReadTransaction();
            RavenTransaction serverTx = null;
            if (Server != null)
                serverTx = Server.OpenReadTransaction();

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

        public static QueryOperationContext ForQuery(DocumentsOperationContext documentsContext, Index index, QueryMetadata queryMetadata)
        {
            return new QueryOperationContext(documentsContext, releaseDocumentsContext: false, needsServerContext: index.Definition.HasCompareExchange || queryMetadata.HasCmpXchg || queryMetadata.HasCmpXchgSelect);
        }

        public static QueryOperationContext ForQuery(DocumentsOperationContext documentsContext, Index index)
        {
            return new QueryOperationContext(documentsContext, releaseDocumentsContext: false, needsServerContext: index.Definition.HasCompareExchange);
        }

        public static QueryOperationContext ForIndex(Index index)
        {
            return new QueryOperationContext(index.DocumentDatabase, index.Definition.HasCompareExchange);
        }

        public static QueryOperationContext Allocate(DocumentDatabase database, bool needsServerContext = false)
        {
            return new QueryOperationContext(database, needsServerContext);
        }

        public static QueryOperationContext ForGraph(DocumentsOperationContext documentsContext)
        {
            return new QueryOperationContext(documentsContext, releaseDocumentsContext: false, needsServerContext: false);
        }

        /// <summary>
        /// For testing purposes only
        /// </summary>
        public static QueryOperationContext ShortTermSingleUse(DocumentDatabase database)
        {
            var documentsContext = DocumentsOperationContext.ShortTermSingleUse(database);

            return new QueryOperationContext(documentsContext, releaseDocumentsContext: true, needsServerContext: false);
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

        private static void ThrowTransactionsNotInTheSameStateException(bool opened, bool serverOpened)
        {
            throw new InvalidOperationException($"Documents transaction ('{opened}') and server transaction ('{serverOpened}') do not have the same state.");
        }
    }
}
