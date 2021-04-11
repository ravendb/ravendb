using System;
using System.Collections.Generic;
using System.IO;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerDestination
    {
        IDisposable Initialize(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion);
        IDatabaseRecordActions DatabaseRecord();
        IDocumentActions Documents(bool throwOnCollectionMismatchError = true);
        IDocumentActions RevisionDocuments();
        IDocumentActions Tombstones();
        IDocumentActions Conflicts();
        IIndexActions Indexes();
        IKeyValueActions<long> Identities();
        ICompareExchangeActions CompareExchange(JsonOperationContext context);
        ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context);
        ICounterActions Counters(SmugglerResult result);
        ISubscriptionActions Subscriptions();
        ITimeSeriesActions TimeSeries();
    }

    public interface IDocumentActions : INewDocumentActions, IDisposable
    {
        void WriteDocument(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress);
        void WriteTombstone(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress);
        void WriteConflict(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress);
        void DeleteDocument(string id);
        IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection();
    }

    public interface INewDocumentActions
    {
        DocumentsOperationContext GetContextForNewDocument();
        Stream GetTempStream();
    }

    public interface IIndexActions : IDisposable
    {
        void WriteIndex(IndexDefinitionBaseServerSide indexDefinition, IndexType indexType);
        void WriteIndex(IndexDefinition indexDefinition);
    }

    public interface ICounterActions : IDisposable, INewDocumentActions
    {
        void WriteCounter(CounterGroupDetail counterDetail);
        void WriteLegacyCounter(CounterDetail counterDetail);
        void RegisterForDisposal(IDisposable data);
    }

    public interface ISubscriptionActions : IDisposable
    {
        void WriteSubscription(SubscriptionState subscriptionState);
    }

    public interface IKeyValueActions<in T> : IDisposable
    {
        void WriteKeyValue(string key, T value);
    }

    public interface ICompareExchangeActions : IDisposable
    {
        void WriteKeyValue(string key, BlittableJsonReaderObject value);
        void WriteTombstoneKey(string key);
    }

    public interface IDatabaseRecordActions : IDisposable
    {
        void WriteDatabaseRecord(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType);
    }

    public interface ITimeSeriesActions : IDisposable
    {
        void WriteTimeSeries(TimeSeriesItem ts);
    }
}
