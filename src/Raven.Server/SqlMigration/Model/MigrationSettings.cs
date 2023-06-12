using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public enum EmbeddedDocumentSqlKeysStorage
    {
        None,
        OnDocument,
        OnMetadata,
    }
    
    public class MigrationSettings
    {
        public List<RootCollection> Collections { get; set; }
        public int BatchSize { get; set; } = 1000;
        public int? MaxRowsPerTable { get; set; }
        public Dictionary<string, EmbeddedDocumentSqlKeysStorage> CollectionsEmbeddedReferencesSqlKeysConfigurations = new();
    }
}
