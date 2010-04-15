using System;
using System.Transactions;

namespace Raven.Client.Document
{
    public class RavenClientEnlistment : IPromotableSinglePhaseNotification
    {
        private readonly IDocumentSession sessionImpl;
        private readonly Guid txId;

        public RavenClientEnlistment(IDocumentSession sessionImpl, Guid txId)
        {
            this.sessionImpl = sessionImpl;
            this.txId = txId;
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
            return txId.ToByteArray();
        }
    }
}