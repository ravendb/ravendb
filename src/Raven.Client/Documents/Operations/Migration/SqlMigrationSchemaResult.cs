namespace Raven.Client.Documents.Operations.Migration
{
    public class SqlMigrationSchemaResult
    {
        public bool Success { get; set; }

        public string Error { get; set; }

        public SqlSchemaResultTable[] Tables { get; set; }
    }

    public class SqlSchemaResultTable
    {
        public string Name { get; set; }
        public Column[] Columns { get; set; }
        public string[] EmbeddedTables { get; set; }

        public class Column
        {
            public string Name { get; set; }
            public ColumnType Type { get; set; }

            public enum ColumnType
            {
                Primary, Foreign, None
            }
        }
    }
}
