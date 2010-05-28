using System;

namespace Raven.Client
{
	public interface ITransactionalDocumentSession
	{
		void Commit(Guid txId);

		void Rollback(Guid txId);
	}
}