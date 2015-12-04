// -----------------------------------------------------------------------
//  <copyright file="EmptyInFlightStateSnapshot.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;

using Raven.Abstractions.Data;

namespace Raven.Database.Impl.DTC
{
    public class EmptyInFlightStateSnapshot : IInFlightStateSnapshot
    {
        public static readonly EmptyInFlightStateSnapshot Instance = new EmptyInFlightStateSnapshot();

        public Func<TDocument, TDocument> GetNonAuthoritativeInformationBehavior<TDocument>(TransactionInformation tx, string key) where TDocument : class, IJsonDocumentMetadata, new()
        {
            return null;
        }
    }
}