using System.Collections.Generic;

namespace Raven.Migrator.MongoDB
{
    public class MongoDBConfiguration : AbstractMigrationConfiguration
    {
        public string ConnectionString { get; set; }

        public bool MigrateGridFS { get; set; }

        public Dictionary<string, string> CollectionsToMigrate { get; set; }
    }
}
