using System;

namespace Raven.Client
{
	public interface ITransactionalDocumentSession
	{
		void Commit(Guid txId);

		void Rollback(Guid txId);

		byte[] PromoteTransaction(Guid fromTxId);
		void StoreRecoveryInformation(Guid txId, byte[] recoveryInformation);
	}
}