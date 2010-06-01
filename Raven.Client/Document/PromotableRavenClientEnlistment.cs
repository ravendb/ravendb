using System;
using System.Transactions;

namespace Raven.Client.Document
{
	public class PromotableRavenClientEnlistment : IPromotableSinglePhaseNotification
	{
		private readonly ITransactionalDocumentSession session;
		private readonly Guid txId = Guid.NewGuid();

		public PromotableRavenClientEnlistment(ITransactionalDocumentSession session)
		{
			this.session = session;
		}

		public byte[] Promote()
		{
			var promoteTransaction = session.PromoteTransaction(txId);
			var tx = TransactionInterop.GetTransactionFromTransmitterPropagationToken(promoteTransaction);
			tx
				.EnlistDurable(
					InMemoryDocumentSessionOperations.RavenDbResourceManagerId,
					new RavenClientEnlistment(session,tx.TransactionInformation.DistributedIdentifier),
					EnlistmentOptions.None
				);
			return promoteTransaction;
		}

		public void Initialize()
		{
		}

		public void SinglePhaseCommit(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			session.Commit(txId);
			singlePhaseEnlistment.Committed();
		}

		public void Rollback(SinglePhaseEnlistment singlePhaseEnlistment)
		{
			session.Rollback(txId);
			singlePhaseEnlistment.Aborted();
		}
	}
}