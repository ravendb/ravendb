using System;
using Raven.Server.Documents;
using Voron.Impl;
using Voron.Schema;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Storage.Schema
{
    public class UpdateStep
    {
        private readonly SchemaUpgradeTransactions _transactions;
        private short? _lastCommittedTxMarker;

        public UpdateStep(SchemaUpgradeTransactions transactions)
        {
            _transactions = transactions;
        }
        public Transaction ReadTx => _transactions.Read;
        public Transaction WriteTx => _transactions.Write;
        public ConfigurationStorage ConfigurationStorage;
        public DocumentsStorage DocumentsStorage;
        public ClusterStateMachine ClusterStateMachine;

        public void Commit(DocumentsOperationContext txContext)
        {
            if (txContext != null)
            {
                if (txContext.GetTransactionMarker() == _lastCommittedTxMarker)
                    throw new InvalidOperationException(
                        $"You're commiting transaction step with the same transaction marker as it was previously committed - {_lastCommittedTxMarker}. Did you forget to set it to write transaction id?");
            }

            _transactions.Commit();

            if (txContext != null)
                _lastCommittedTxMarker = txContext.GetTransactionMarker();
        }

        public void RenewTransactions()
        {
            _transactions.Renew();
        }
    }
}
