using System;
using System.Collections.Generic;
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
using Raven.Server.Documents.PeriodicBackup;
using Raven.Server.Routing;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerDestination
    {
        ValueTask<IAsyncDisposable> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, long buildVersion);

        IDatabaseRecordActions DatabaseRecord();

        IDocumentActions Documents(bool throwOnCollectionMismatchError = true);

        IDocumentActions RevisionDocuments();

        IDocumentActions Tombstones();

        IDocumentActions Conflicts();

        IIndexActions Indexes();

        IKeyValueActions<long> Identities();

        ICompareExchangeActions CompareExchange(JsonOperationContext context, BackupKind? backupKind, bool withDocuments);

        ICompareExchangeActions CompareExchangeTombstones(JsonOperationContext context);

        ICounterActions Counters(SmugglerResult result);

        ICounterActions LegacyCounters(SmugglerResult result);

        ISubscriptionActions Subscriptions();

        IReplicationHubCertificateActions ReplicationHubCertificates();

        ITimeSeriesActions TimeSeries();

        ILegacyActions LegacyDocumentDeletions();

        ILegacyActions LegacyAttachmentDeletions();
    }

    public interface IDocumentActions : INewDocumentActions
    {
        ValueTask WriteDocumentAsync(DocumentItem item, SmugglerProgressBase.CountsWithLastEtagAndAttachments progress);

        ValueTask WriteTombstoneAsync(Tombstone tombstone, SmugglerProgressBase.CountsWithLastEtag progress);

        ValueTask WriteConflictAsync(DocumentConflict conflict, SmugglerProgressBase.CountsWithLastEtag progress);

        ValueTask DeleteDocumentAsync(string id);
        IEnumerable<DocumentItem> GetDocumentsWithDuplicateCollection();
    }

    public interface INewCompareExchangeActions
    {
        JsonOperationContext GetContextForNewCompareExchangeValue();
    }

    public interface INewDocumentActions : IAsyncDisposable
    {
        JsonOperationContext GetContextForNewDocument();

        Stream GetTempStream();
    }

    public interface IIndexActions : IAsyncDisposable
    {
        ValueTask WriteIndexAsync(IndexDefinitionBaseServerSide indexDefinition, IndexType indexType);

        ValueTask WriteIndexAsync(IndexDefinition indexDefinition);
    }

    public interface ICounterActions : INewDocumentActions
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

    public interface ICompareExchangeActions : INewCompareExchangeActions, IAsyncDisposable
    {
        ValueTask WriteKeyValueAsync(string key, BlittableJsonReaderObject value, Document existingDocument);

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

    public interface ILegacyActions : IAsyncDisposable
    {
        ValueTask WriteLegacyDeletions(string id);
    }


}
