using System;
using Raven.Abstractions.Data;

namespace Raven.Database.Impl.DTC
{
    public interface IInFlightStateSnapshot
    {
        Func<TDocument, TDocument> GetNonAuthoritativeInformationBehavior<TDocument>(TransactionInformation tx, string key) where TDocument : class, IJsonDocumentMetadata, new();
    }
}