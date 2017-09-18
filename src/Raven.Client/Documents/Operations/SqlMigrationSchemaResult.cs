
namespace Raven.Client.Documents.Operations
{
    public class SqlMigrationSchemaResult
    {
        public bool Success;

        public string Errors;

        public string[] Tables { get; set; }
    }
}
