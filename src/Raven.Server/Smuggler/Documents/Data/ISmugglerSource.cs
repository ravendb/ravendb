using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;
using Raven.Server.Documents;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerSource
    {
        IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion);
        DatabaseItemType GetNextType();
        IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentItem> GetLegacyAttachments(INewDocumentActions actions);
        IEnumerable<string> GetLegacyAttachmentDeletions();
        IEnumerable<string> GetLegacyDocsDeletions();
        IEnumerable<DocumentTombstone> GetTombstones(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentConflict> GetConflicts(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<IndexDefinitionAndType> GetIndexes();
        IDisposable GetIdentities(out IEnumerable<(string Prefix, long Value)> identities);
        IDisposable GetCmpXchg(out IEnumerable<(string key, long index, BlittableJsonReaderObject value)> cmpXchg);
        long SkipType(DatabaseItemType type, Action<long> onSkipped);
    }

    public class IndexDefinitionAndType
    {
        public object IndexDefinition;

        public IndexType Type;
    }
}
