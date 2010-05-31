using System;
using System.Transactions;

namespace Raven.Client.Document
{
    public class RavenClientEnlistment : IPromotableSinglePhaseNotification
    {
		private readonly ITransactionalDocumentSession sessionImpl;
        private Guid txId;
    	private CommittableTransaction promotedTx;

    	public RavenClientEnlistment(ITransactionalDocumentSession sessionImpl, Guid txId)
        {
            this.sessionImpl = sessionImpl;
            this.txId = txId;
			if (this.txId == Guid.Empty)
				this.txId = Guid.NewGuid();
        }

        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
            preparingEnlistment.Prepared();
        }

        public void Commit(Enlistment enlistment)
        {
            sessionImpl.Commit(txId);
            enlistment.Done();
        }

        public void Rollback(Enlistment enlistment)
        {
            sessionImpl.Rollback(txId);
            enlistment.Done();
        }

        public void InDoubt(Enlistment enlistment)
        {
            sessionImpl.Rollback(txId);
            enlistment.Done();
        }

        public void Initialize()
        {
        }

        public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
        {
            sessionImpl.Commit(txId);
            singlePhaseEnlistment.Committed();
        }

        public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
        {
            sessionImpl.Rollback(txId);
            singlePhaseEnlistment.Aborted();
        }

    	public byte[] Promote()
    	{
    		promotedTx = new CommittableTransaction();
    		var propagationToken = TransactionInterop.GetTransmitterPropagationToken(promotedTx);
    		sessionImpl.PromoteTransaction(txId, promotedTx.TransactionInformation.DistributedIdentifier);
    		txId = promotedTx.TransactionInformation.DistributedIdentifier;
    		return propagationToken;
    	}
    }
}