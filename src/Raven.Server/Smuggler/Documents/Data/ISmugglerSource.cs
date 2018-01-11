using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerSource
    {
        IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion);
        DatabaseItemType GetNextType();
        DatabaseRecord GetDatabaseRecord();
        IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentItem> GetLegacyAttachments(INewDocumentActions actions);
        IEnumerable<string> GetLegacyAttachmentDeletions();
        IEnumerable<string> GetLegacyDocumentDeletions();
        IEnumerable<DocumentTombstone> GetTombstones(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentConflict> GetConflicts(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<IndexDefinitionAndType> GetIndexes();
        IDisposable GetIdentities(out IEnumerable<(string Prefix, long Value)> identities);
        IDisposable GetCompareExchangeValues(out IEnumerable<(string key, long index, BlittableJsonReaderObject value)> compareExchange);
        long SkipType(DatabaseItemType type, Action<long> onSkipped);
    }

    public class IndexDefinitionAndType
    {
        public object IndexDefinition;

        public IndexType Type;
    }
}
