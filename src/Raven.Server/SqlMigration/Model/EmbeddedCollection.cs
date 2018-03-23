namespace Raven.Server.SqlMigration.Model
{
    public class EmbeddedCollection : CollectionWithReferences
    {
        public EmbeddedCollection(string sourceTableSchema, string sourceTableName, string name) : base(sourceTableSchema, sourceTableName, name)
        {
        }
    }
}
