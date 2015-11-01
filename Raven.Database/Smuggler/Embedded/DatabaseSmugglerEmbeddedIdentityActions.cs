using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
    public class DatabaseSmugglerEmbeddedIdentityActions : IDatabaseSmugglerIdentityActions
    {
        private readonly DocumentDatabase _database;

        public DatabaseSmugglerEmbeddedIdentityActions(DocumentDatabase database)
        {
            _database = database;
        }

        public void Dispose()
        {
        }

        public Task WriteIdentityAsync(string name, long value, CancellationToken cancellationToken)
        {
            _database.TransactionalStorage.Batch(accessor => accessor.General.SetIdentityValue(name, value));
            return new CompletedTask();
        }
    }
}
