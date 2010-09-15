using System.Transactions;

namespace Raven.Client.Document
{
	/// <summary>
	/// An implementation of <see cref="IEnlistmentNotification"/> for the Raven Client API, allowing Raven
	/// Client API to participate in Distributed Transactions
	/// </summary>
    public class RavenClientEnlistment : IEnlistmentNotification
    {
		private readonly ITransactionalDocumentSession session;
		private readonly TransactionInformation transaction;

		/// <summary>
		/// Initializes a new instance of the <see cref="RavenClientEnlistment"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
    	public RavenClientEnlistment(ITransactionalDocumentSession session)
        {
    		transaction = Transaction.Current.TransactionInformation;
            this.session = session;
        }

		/// <summary>
		/// Notifies an enlisted object that a transaction is being prepared for commitment.
		/// </summary>
		/// <param name="preparingEnlistment">A <see cref="T:System.Transactions.PreparingEnlistment"/> object used to send a response to the transaction manager.</param>
        public void Prepare(PreparingEnlistment preparingEnlistment)
        {
        	session.StoreRecoveryInformation(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction), preparingEnlistment.RecoveryInformation());
            preparingEnlistment.Prepared();
        }

		/// <summary>
		/// Notifies an enlisted object that a transaction is being committed.
		/// </summary>
		/// <param name="enlistment">An <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
        public void Commit(Enlistment enlistment)
        {
			session.Commit(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            enlistment.Done();
        }

		/// <summary>
		/// Notifies an enlisted object that a transaction is being rolled back (aborted).
		/// </summary>
		/// <param name="enlistment">A <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
        public void Rollback(Enlistment enlistment)
        {
			session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            enlistment.Done();
        }

		/// <summary>
		/// Notifies an enlisted object that the status of a transaction is in doubt.
		/// </summary>
		/// <param name="enlistment">An <see cref="T:System.Transactions.Enlistment"/> object used to send a response to the transaction manager.</param>
        public void InDoubt(Enlistment enlistment)
        {
			session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            enlistment.Done();
        }

		/// <summary>
		/// Initializes this instance.
		/// </summary>
        public void Initialize()
        {
        }

		/// <summary>
		/// Rollbacks the specified single phase enlistment.
		/// </summary>
		/// <param name="singlePhaseEnlistment">The single phase enlistment.</param>
        public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
        {
			session.Rollback(PromotableRavenClientEnlistment.GetLocalOrDistributedTransactionId(transaction));
            singlePhaseEnlistment.Aborted();
        }
    }
}