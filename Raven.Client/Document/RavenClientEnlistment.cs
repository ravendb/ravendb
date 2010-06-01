using System;
using System.Transactions;

namespace Raven.Client.Document
{
    public class RavenClientEnlistment : IEnlistmentNotification
    {
		private readonly ITransactionalDocumentSession session;
		private readonly TransactionInformation transaction;

    	public RavenClientEnlistment(ITransactionalDocumentSession session)
        {
    		transaction = Transaction.Current.TransactionInformation;
            this.session = session;
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
        	session.StoreRecoveryInformation(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction), preparingEnlistment.RecoveryInformation());
            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
			session.Commit(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
			session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
			session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            enlistment.Done();
        }

        public void Initialize()
        {
        }

        public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
        {
			session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            singlePhaseEnlistment.Aborted();
        }
    }
}