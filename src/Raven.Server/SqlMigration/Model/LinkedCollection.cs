namespace Raven.Server.SqlMigration.Model
{
    public class LinkedCollection : AbstractCollection
    {
        public LinkedCollection(string sourceTableSchema, string sourceTableName, string name) : base(sourceTableSchema, sourceTableName, name)
        {
        }
    }
}
