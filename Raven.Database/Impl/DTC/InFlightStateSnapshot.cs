// -----------------------------------------------------------------------
//  <copyright file="InFlightStateSnapshot.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Immutable;

using Raven.Abstractions.Data;
using Raven.Json.Linq;

namespace Raven.Database.Impl.DTC
{
    public class InFlightStateSnapshot : IInFlightStateSnapshot
    {
        private readonly ImmutableDictionary<string, ChangedDoc> changedInTransaction;

        private readonly ImmutableDictionary<string, TransactionState> transactionStates;

        public InFlightStateSnapshot(ImmutableDictionary<string, ChangedDoc> changedInTransaction, ImmutableDictionary<string, TransactionState> transactionStates)
        {
            this.changedInTransaction = changedInTransaction;
            this.transactionStates = transactionStates;
        }

        public Func<TDocument, TDocument> GetNonAuthoritativeInformationBehavior<TDocument>(TransactionInformation tx, string key) where TDocument : class, IJsonDocumentMetadata, new()
        {
            ChangedDoc existing;

            if (changedInTransaction.TryGetValue(key, out existing) == false || (tx != null && tx.Id == existing.transactionId))
                return null;

            if (transactionStates.ContainsKey(existing.transactionId) == false)
                return null; // shouldn't happen, but we have better be on the safe side

            return document =>
            {
                if (document == null)
                {
                    return new TDocument
                    {
                        Key = key,
                        Metadata = new RavenJObject { { Constants.RavenDocumentDoesNotExists, true } },
                        LastModified = DateTime.MinValue,
                        NonAuthoritativeInformation = true,
                        Etag = Etag.Empty
                    };
                }

                document.NonAuthoritativeInformation = true;
                return document;
            };
        }
    }
}