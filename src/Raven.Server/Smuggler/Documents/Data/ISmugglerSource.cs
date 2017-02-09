using System;
using System.Collections.Generic;
using Raven.NewClient.Abstractions.Indexing;
using Raven.NewClient.Data.Indexes;
using Raven.NewClient.Client.Smuggler;
using Raven.Server.Documents;

namespace Raven.Server.Smuggler.Documents.Data
{
    public interface ISmugglerSource
    {
        IDisposable Initialize(DatabaseSmugglerOptions options, SmugglerResult result, out long buildVersion);
        DatabaseItemType GetNextType();
        IEnumerable<Document> GetDocuments(List<string> collectionsToExport, INewDocumentActions actions);
        IEnumerable<Document> GetRevisionDocuments(List<string> collectionsToExport, INewDocumentActions actions, int limit);
        IEnumerable<IndexDefinitionAndType> GetIndexes();
        IEnumerable<TransformerDefinition> GetTransformers();
        IEnumerable<KeyValuePair<string, long>> GetIdentities();
        long SkipType(DatabaseItemType type);
    }

    public class IndexDefinitionAndType
    {
        public object IndexDefinition;

        public IndexType Type;
    }
}