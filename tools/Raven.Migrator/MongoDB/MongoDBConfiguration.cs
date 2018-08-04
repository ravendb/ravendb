namespace Raven.Migrator.MongoDB
{
    public class MongoDBConfiguration : AbstractMigrationConfiguration
    {
        public string ConnectionString { get; set; }

        public bool MigrateGridFS { get; set; }
    }
}
