using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Replication;
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
        IAsyncDisposable InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion);

        IDatabaseRecordActions DatabaseRecord();

        IDocumentActions Documents();

        IDocumentActions RevisionDocuments();

        IDocumentActions Tombstones();

        IDocumentActions Conflicts();

        IIndexActions Indexes();

        IKeyValueActions<long> Identities();

        ICompareExchangeActions CompareExchange(JsonOperationContext context);

        ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context);

        ICounterActions Counters(SmugglerResult result);

        ISubscriptionActions Subscriptions();

        IReplicationHubCertificateActions ReplicationHubCertificates();

        ITimeSeriesActions TimeSeries();
    }

    public interface IDocumentActions : INewDocumentActions, IAsyncDisposable
    {
        ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress);

        ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress);

        ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress);

        ValueTask DeleteDocumentAsync(string id);
    }

    public interface INewDocumentActions
    {
        DocumentsOperationContext GetContextForNewDocument();

        Stream GetTempStream();
    }

    public interface IIndexActions : IAsyncDisposable
    {
        ValueTask WriteIndexAsync(IndexDefinitionBase indexDefinition, IndexType indexType);

        ValueTask WriteIndexAsync(IndexDefinition indexDefinition);
    }

    public interface ICounterActions : IAsyncDisposable, INewDocumentActions
    {
        ValueTask WriteCounterAsync(CounterGroupDetail counterDetail);

        ValueTask WriteLegacyCounterAsync(CounterDetail counterDetail);

        void RegisterForDisposal(IDisposable data);
    }

    public interface ISubscriptionActions : IAsyncDisposable
    {
        ValueTask WriteSubscriptionAsync(SubscriptionState subscriptionState);
    }

    public interface IReplicationHubCertificateActions : IAsyncDisposable
    {
        ValueTask WriteReplicationHubCertificateAsync(string hub, ReplicationHubAccess access);
    }

    public interface IKeyValueActions<in T> : IAsyncDisposable
    {
        ValueTask WriteKeyValueAsync(string key, T value);
    }

    public interface ICompareExchangeActions : IAsyncDisposable
    {
        ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value);

        ValueTask WriteTombstoneKeyAsync(string key);
    }

    public interface IDatabaseRecordActions : IAsyncDisposable
    {
        ValueTask WriteDatabaseRecordAsync(DatabaseRecord databaseRecord, SmugglerProgressBase.DatabaseRecordProgress progress, AuthorizationStatus authorizationStatus, DatabaseRecordItemType databaseRecordItemType);
    }

    public interface ITimeSeriesActions : IAsyncDisposable
    {
        ValueTask WriteTimeSeriesAsync(TimeSeriesItem ts);
    }
}
