// -----------------------------------------------------------------------
//  <copyright file="DtcNotSupportedTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Impl.DTC
{
    internal class DtcNotSupportedTransactionalState : InFlightTransactionalState
    {
        private readonly string storageName;

        public DtcNotSupportedTransactionalState(string storageName,
            Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut,
            Func<string, Etag, TransactionInformation, bool> databaseDelete)
            : base(databasePut, databaseDelete, false)
        {
            this.storageName = storageName;
        }

        public override void Commit(string id)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public override void Prepare(string id, Guid? resourceManagerId, byte[] recoveryInformation)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public override void Rollback(string id)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public override Etag AddDocumentInTransaction(
            string key,
            Etag etag,
            RavenJObject data,
            RavenJObject metadata,
            TransactionInformation transactionInformation,
            Etag committedEtag,
            IUuidGenerator uuidGenerator)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public override void DeleteDocumentInTransaction(
            TransactionInformation transactionInformation,
            string key,
            Etag etag,
            Etag committedEtag,
            IUuidGenerator uuidGenerator)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public override bool IsModified(string key)
        {
            return false;
        }

        public override IInFlightStateSnapshot GetSnapshot()
        {
            return EmptyInFlightStateSnapshot.Instance;
        }

        public override bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocument document)
        {
            document = null;
            return false;
        }

        public override bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocumentMetadata document)
        {
            document = null;
            return false;
        }

        public override bool HasTransaction(string txId)
        {
            return false;
        }
    }
}
