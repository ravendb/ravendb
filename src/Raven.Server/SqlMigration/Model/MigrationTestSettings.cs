using System.Collections.Generic;

namespace Raven.Server.SqlMigration.Model
{
    public class MigrationTestSettings
    {
        public bool BinaryToAttachment { get; set; }
        public RootCollection Collection { get; set; }
        public MigrationTestMode Mode { get; set; }
        public string[] PrimaryKeyValues { get; set; }
        public List<CollectionNamesMapping> CollectionsMapping { get; set; }
    }
    
    public enum MigrationTestMode
    {
        First,
        ByPrimaryKey
    }
    
    public class CollectionNamesMapping {
        public string TableSchema { get; set; }
        public string TableName { get; set; }
        public string CollectionName { get; set; }
    }
}
