using Raven.Server.Documents;
using Voron;
using Voron.Impl;
using Voron.Schema;
using Raven.Server.ServerWide;

namespace Raven.Server.Storage.Schema
{
    public class UpdateStep
    {
        private readonly SchemaUpgradeTransactions _transactions;

        public UpdateStep(SchemaUpgradeTransactions transactions)
        {
            _transactions = transactions;
        }
        public Transaction ReadTx => _transactions.Read;
        public Transaction WriteTx => _transactions.Write;
        public ConfigurationStorage ConfigurationStorage;
        public DocumentsStorage DocumentsStorage;
        public ClusterStateMachine ClusterStateMachine;

        public void Commit()
        {
            _transactions.Commit();
        }

        public void RenewTransactions()
        {
            _transactions.Renew();
        }
    }
}
