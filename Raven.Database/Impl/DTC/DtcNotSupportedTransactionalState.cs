// -----------------------------------------------------------------------
//  <copyright file="DtcNotSupportedTransactionalState.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using Raven.Abstractions.Data;
using Raven.Database.Storage;
using Raven.Database.Util;
using Raven.Json.Linq;

namespace Raven.Database.Impl.DTC
{
    public class DtcNotSupportedTransactionalState : InFlightTransactionalState
    {
        private readonly string storageName;

        public DtcNotSupportedTransactionalState(string storageName,
            Func<string, Etag, RavenJObject, RavenJObject, TransactionInformation, PutResult> databasePut,
            Func<string, Etag, TransactionInformation, bool> databaseDelete)
            : base(databasePut, databaseDelete)
        {
            this.storageName = storageName;
        }

        public override void Commit(string id)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public override void Prepare(string id)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public override void Rollback(string id)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public new Etag AddDocumentInTransaction(
            string key,
            Etag etag,
            RavenJObject data,
            RavenJObject metadata,
            TransactionInformation transactionInformation,
            Etag committedEtag,
            SequentialUuidGenerator uuidGenerator)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public new void DeleteDocumentInTransaction(
            TransactionInformation transactionInformation,
            string key,
            Etag etag,
            Etag committedEtag,
            SequentialUuidGenerator uuidGenerator)
        {
            throw new InvalidOperationException("DTC is not supported by " + storageName + " storage.");
        }

        public new bool IsModified(string key)
        {
            return false;
        }

        public new Func<TDocument, TDocument> GetNonAuthoritativeInformationBehavior<TDocument>(TransactionInformation tx,
                                                                                            string key)
            where TDocument : class, IJsonDocumentMetadata, new()
        {
            return null;
        }

        public new bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocument document)
        {
            document = null;
            return false;
        }

        public new bool TryGet(string key, TransactionInformation transactionInformation, out JsonDocumentMetadata document)
        {
            document = null;
            return false;
        }

        public new bool HasTransaction(string txId)
        {
            return false;
        }
    }
}