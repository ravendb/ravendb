using System;
using System.Linq;
using System.Transactions;

namespace Raven.Client.Document
{
	/// <summary>
	/// An implementation of <see cref="IPromotableSinglePhaseNotification"/> for RavenDB Client API
	/// </summary>
	public class PromotableRavenClientEnlistment : IPromotableSinglePhaseNotification
	{
		private readonly ITransactionalDocumentSession session;
		private readonly TransactionInformation transaction;

		/// <summary>
		/// Initializes a new instance of the <see cref="PromotableRavenClientEnlistment"/> class.
		/// </summary>
		/// <param name="session">The session.</param>
		public PromotableRavenClientEnlistment(ITransactionalDocumentSession session)
		{
			transaction = Transaction.Current.TransactionInformation;
			this.session = session;
		}

		public byte[] Promote()
		{
			return session.PromoteTransaction(GetLocalOrDistributedTransactionId(transaction));
		}

		public void Initialize()
		{
		}

		public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			session.Commit(GetLocalOrDistributedTransactionId(transaction));
			singlePhaseEnlistment.Committed();
		}

		public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			session.Rollback(GetLocalOrDistributedTransactionId(transaction));
			singlePhaseEnlistment.Aborted();
		}

		public static Guid GetLocalOrDistributedTransactionId(TransactionInformation transactionInformation)
		{
			if (transactionInformation.DistributedIdentifier != Guid.Empty)
				return transactionInformation.DistributedIdentifier;
			var first = transactionInformation.LocalIdentifier.Split(':').First();
			return new Guid(first);
		}
	}
}