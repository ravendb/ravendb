using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Abstractions.Data;
using Raven.Database.Impl.DTC;
using Raven.Json.Linq;
using Raven.Storage.Voron;

namespace Raven.Database.Storage.Voron
{
    public class VoronInFlightTransactionalState : InFlightTransactionalState
    {
        private readonly TransactionalStorage storage;

        public VoronInFlightTransactionalState(Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut, Func<string, Etag, TransactionInformation, bool> databaseDelete, TransactionalStorage storage) : base(databasePut, databaseDelete)
        {
            this.storage = storage;
        }

        public override void Commit(string id)
        {
            storage.Batch(accessor => RunOperationsInTransaction(id));
        }

        public override void Prepare(string id)
        {
        }
    }
}
