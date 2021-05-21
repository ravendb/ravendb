using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Counters;
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
        IDisposable Initialize(DatabaseSmugglerOptionsServerSide options, SmugglerResult result, out long buildVersion);
        DatabaseItemType GetNextType();
        DatabaseRecord GetDatabaseRecord();
        IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentItem> GetLegacyAttachments(INewDocumentActions actions);
        IEnumerable<string> GetLegacyAttachmentDeletions();
        IEnumerable<string> GetLegacyDocumentDeletions();
        IEnumerable<Tombstone> GetTombstones(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentConflict> GetConflicts(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<IndexDefinitionAndType> GetIndexes();
        IEnumerable<(string Prefix, long Value, long Index)> GetIdentities();
        IEnumerable<(CompareExchangeKey Key, long Index, BlittableJsonReaderObject Value)> GetCompareExchangeValues(INewCompareExchangeActions actions);
        IEnumerable<(CompareExchangeKey Key, long Index)> GetCompareExchangeTombstones();
        IEnumerable<CounterGroupDetail> GetCounterValues(List<string> collectionsToExport, ICounterActions actions);
        IEnumerable<CounterDetail> GetLegacyCounterValues();
        IEnumerable<SubscriptionState> GetSubscriptions();
        IEnumerable<TimeSeriesItem> GetTimeSeries(List<string> collectionsToExport);

        long SkipType(DatabaseItemType type, Action<long> onSkipped, CancellationToken token);

        SmugglerSourceType GetSourceType();
        
    }
    public enum SmugglerSourceType
    {
        None,
        FullExport,
        IncrementalExport,
        Import
    }

    public class IndexDefinitionAndType
    {
        public object IndexDefinition;

        public IndexType Type;
    }
}
