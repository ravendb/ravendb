
namespace Raven.Server.SqlMigration
{
    public class SqlParentTable : SqlTable
    {
        public JsPatch Patcher;
        public readonly string PatchScript;

        public SqlParentTable(string tableName, string query, SqlDatabase database, string newName, string patch) : base(tableName, query, database, newName)
        {
            PatchScript = string.IsNullOrEmpty(patch) ? null : patch;
            IsEmbedded = false;
        }

        public JsPatch GetJsPatch()
        {
            return Patcher ?? (Patcher = new JsPatch(PatchScript, Database.Context));
        }

        public override SqlReader GetReader()
        {
            if (Reader != null)
                return Reader;

            var query = InitialQuery + SqlQueries.OrderByColumns(PrimaryKeys);
            Reader = new SqlReader(Database.Connection, query);
            return Reader;
        }
    }
}
