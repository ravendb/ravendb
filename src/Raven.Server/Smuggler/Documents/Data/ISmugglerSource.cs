using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.Documents.Operations.Replication;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerSource
    {
        Task<SmugglerInitializeResult> InitializeAsync(DatabaseSmugglerOptionsServerSide options, SmugglerResult result);

        Task<DatabaseItemType> GetNextTypeAsync();

        Task<DatabaseRecord> GetDatabaseRecordAsync();

        IAsyncEnumerable<DocumentItem> GetDocumentsAsync(List<string> collectionsToExport, INewDocumentActions actions);

        IAsyncEnumerable<DocumentItem> GetRevisionDocumentsAsync(List<string> collectionsToExport, INewDocumentActions actions);

        IAsyncEnumerable<DocumentItem> GetLegacyAttachmentsAsync(INewDocumentActions actions);

        IAsyncEnumerable<string> GetLegacyAttachmentDeletionsAsync();

        IAsyncEnumerable<string> GetLegacyDocumentDeletionsAsync();

        IAsyncEnumerable<Tombstone> GetTombstonesAsync(List<string> collectionsToExport, INewDocumentActions actions);

        IAsyncEnumerable<DocumentConflict> GetConflictsAsync(List<string> collectionsToExport, INewDocumentActions actions);

        IAsyncEnumerable<IndexDefinitionAndType> GetIndexesAsync();

        IAsyncEnumerable<(string Prefix, long Value, long Index)> GetIdentitiesAsync();

        IAsyncEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValuesAsync();

        IAsyncEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstonesAsync();

        IAsyncEnumerable<CounterGroupDetail> GetCounterValuesAsync(List<string> collectionsToExport, ICounterActions actions);

        IAsyncEnumerable<CounterDetail> GetLegacyCounterValuesAsync();

        IAsyncEnumerable<SubscriptionState> GetSubscriptionsAsync();

        IAsyncEnumerable<(string Hub, ReplicationHubAccess Access)> GetReplicationHubCertificatesAsync();

        IAsyncEnumerable<TimeSeriesItem> GetTimeSeriesAsync(List<string> collectionsToExport);

        Task<long> SkipTypeAsync(DatabaseItemType type, Action<long> onSkipped, CancellationToken token);

        SmugglerSourceType GetSourceType();
    }

    public enum SmugglerSourceType
    {
        None,
        FullExport,
        IncrementalExport,
        Import
    }

    public class SmugglerInitializeResult : IDisposable
    {
        private readonly IDisposable _disposable;

        public readonly long BuildNumber;

        public SmugglerInitializeResult(IDisposable disposable, long buildNumber)
        {
            _disposable = disposable ?? throw new ArgumentNullException(nameof(disposable));
            BuildNumber = buildNumber;
        }

        public void Dispose()
        {
            _disposable.Dispose();
        }
    }

    public class IndexDefinitionAndType
    {
        public object IndexDefinition;

        public IndexType Type;
    }
}
