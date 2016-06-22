using System;
using Raven.Client.Smuggler;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Smuggler
{
    public class DatabaseDataImporter
    {
        private readonly DocumentDatabase _database;

        public DatabaseDataImporter(DocumentDatabase database)
        {
            _database = database;
        }

        public ExportResult Import(DocumentsOperationContext context, IDatabaseSmugglerDestination destination)
        {
            throw new NotImplementedException();
        }
    }
}