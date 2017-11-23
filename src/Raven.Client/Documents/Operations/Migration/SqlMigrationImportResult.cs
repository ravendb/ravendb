namespace Raven.Client.Documents.Operations.Migration
{
    public class SqlMigrationImportResult
    {
        public bool Success { get; set; }

        public Error[] Errors { get; set; }

        public class Error
        {
            public ErrorType Type { get; set; }
            public string Message { get; set; }
            public string TableName { get; set; }
            public string ColumnName { get; set; }

            public enum ErrorType
            {
                TableMissingName,
                TableNotExist,
                DuplicateName,
                DuplicateParentTable,
                InvalidPatch,
                TableMissingPrimaryKeys,
                InvalidOrderBy,
                InvalidQuery,
                InvalidEmbed,
                UnsupportedType,
                BadConnectionString,
                ParseError
            }
        }
    }
}
