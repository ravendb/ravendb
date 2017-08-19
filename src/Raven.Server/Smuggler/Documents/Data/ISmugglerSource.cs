using System;
using System.Collections.Generic;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Smuggler;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerSource
    {
        IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion);
        DatabaseItemType GetNextType();
        IEnumerable<DocumentItem> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<DocumentItem> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<IndexDefinitionAndType> GetIndexes();
        IEnumerable<KeyValuePair<string, long>> GetIdentities();
        long SkipType(DatabaseItemType type);
    }

    public class IndexDefinitionAndType
    {
        public object IndexDefinition;

        public IndexType Type;
    }
}
