using System;
using System.Transactions;

namespace Raven.Client.Document
{
    public class RavenClientEnlistment : IEnlistmentNotification
    {
		private readonly ITransactionalDocumentSession session;
        private readonly Guid txId;

    	public RavenClientEnlistment(ITransactionalDocumentSession session, Guid txId)
        {
            this.session = session;
            this.txId = txId;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
        	session.StoreRecoveryInformation(txId, preparingEnlistment.RecoveryInformation());
            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            session.Commit(txId);
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            session.Rollback(txId);
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            session.Rollback(txId);
            enlistment.Done();
        }

        public void Initialize()
        {
        }

        public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
        {
            session.Rollback(txId);
            singlePhaseEnlistment.Aborted();
        }
    }
}