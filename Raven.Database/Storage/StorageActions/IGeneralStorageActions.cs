using System;
using System.Collections.Generic;
using Microsoft.Isam.Esent.Interop;

namespace Raven.Database.Storage.StorageActions
{
	public interface IGeneralStorageActions
	{
		event Action OnCommit;
		void Commit(CommitTransactionGrbit txMode);
		int GetNextIdentityValue(string name);
		void RollbackTransaction(Guid txId);
		void ModifyTransactionId(Guid fromTxId, Guid toTxId, TimeSpan timeout);
		IEnumerable<Guid> GetTransactionIds();
		void CompleteTransaction(Guid txId, Action<DocumentInTransactionData> perDocumentModified);
	}
}