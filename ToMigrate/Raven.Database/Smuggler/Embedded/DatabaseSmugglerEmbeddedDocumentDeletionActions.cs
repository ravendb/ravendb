using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
    public class DatabaseSmugglerEmbeddedDocumentDeletionActions : IDatabaseSmugglerDocumentDeletionActions
    {
        private readonly DocumentDatabase _database;

        public DatabaseSmugglerEmbeddedDocumentDeletionActions(DocumentDatabase database)
        {
            _database = database;
        }

        public void Dispose()
        {
        }

        public Task WriteDocumentDeletionAsync(string key, CancellationToken cancellationToken)
        {
            _database.Documents.Delete(key, null);
            return new CompletedTask();
        }
    }
}
