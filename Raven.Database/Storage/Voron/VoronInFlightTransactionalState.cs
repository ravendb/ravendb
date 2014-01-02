using System;
using System.Collections.Concurrent;
using System.Threading;
using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Database.Impl.DTC;
using Raven.Database.Util;
using Raven.Json.Linq;
using Raven.Storage.Voron;

namespace Raven.Database.Storage.Voron
{
    public class VoronInFlightTransactionalState : InFlightTransactionalState
    {
        private readonly TransactionalStorage storage;
	    private readonly ConcurrentSet<string> preparedTransactions;

        public VoronInFlightTransactionalState(Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, Func<string, Etag, TransactionInformation, bool> databaseDelete, TransactionalStorage storage) : base(databasePut, databaseDelete)
        {
            this.storage = storage;
			preparedTransactions = new ConcurrentSet<string>();   
        }

	    public override void Rollback(string txId)
	    {
		    base.Rollback(txId);
		    preparedTransactions.TryRemove(txId);
	    }

	    public override void Commit(string txId)
        {
			//the exception is thrown here for compatibility reasons - with Esent DTC implementation
			if (!preparedTransactions.Contains(txId))
				throw new InvalidOperationException(String.Format("There is no transaction with id: {0} ready to commit. Did you call PrepareTransaction?", txId));

			try
			{
				storage.Batch(accessor => RunOperationsInTransaction(txId));
			}
			catch (Exception)
			{
				Rollback(txId);
				throw;
			}
		}

        public override void Prepare(string txId)
        {
			//in Voron there has no support for two-phase commit - so no work on Voron itself is done during this stage
			preparedTransactions.Add(txId);
		}
    }
}
