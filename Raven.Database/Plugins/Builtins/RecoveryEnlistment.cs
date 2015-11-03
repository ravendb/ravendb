using System;
using System.Transactions;

namespace Raven.Database.Plugins.Builtins
{
    public class RecoveryEnlistment : IEnlistmentNotification
    {
        private readonly DocumentDatabase database;
        private readonly string transactionId;

        public RecoveryEnlistment(DocumentDatabase database, string transactionId)
        {
            this.database = database;
            this.transactionId = transactionId;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared(); // should not really be called, and we did the work already anyway
        }

        public void Commit(Enlistment enlistment)
        {
            database.Commit(transactionId);
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            database.Rollback(transactionId);
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            database.Rollback(transactionId);
            enlistment.Done();
        }
    }
}
