using Raven.Client.Documents.Conventions;
using Raven.Client.ServerWide;

namespace Raven.Embedded
{
    public sealed class DatabaseOptions
    {
        public bool SkipCreatingDatabase { get; set; }

        public DocumentConventions Conventions { get; set; }

        public DatabaseRecord DatabaseRecord { get; }

        public DatabaseOptions(string databaseName) : this(new DatabaseRecord(databaseName))
        {
        }

        public DatabaseOptions(DatabaseRecord databaseRecord)
        {
            DatabaseRecord = databaseRecord;
        }
    }
}
