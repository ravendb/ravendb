using System;

namespace Raven.Client
{
	public interface ITransactionalDocumentSession
	{
		void Commit(Guid txId);

		void Rollback(Guid txId);

		void PromoteTransaction(Guid fromTxId, Guid toTxId);
	}
}