using System;
using Raven.Client.ServerWide;

namespace Raven.Embedded
{
    public class DatabaseOptions
    {
        public bool SkipCreatingDatabase { get; set; }

        public DatabaseRecord DatabaseRecord { get; private set; }

        public DatabaseOptions(string databaseName) : this(new DatabaseRecord(databaseName))
        {
        }

        public DatabaseOptions(DatabaseRecord databaseRecord)
        {
            DatabaseRecord = databaseRecord;
        }

    }
}
