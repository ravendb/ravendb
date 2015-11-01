using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
    public class DatabaseSmugglerEmbeddedIndexActions : IDatabaseSmugglerIndexActions
    {
        private readonly DocumentDatabase _database;

        public DatabaseSmugglerEmbeddedIndexActions(DocumentDatabase database)
        {
            _database = database;
        }

        public void Dispose()
        {
        }

        public Task WriteIndexAsync(IndexDefinition index, CancellationToken cancellationToken)
        {
            _database.Indexes.PutIndex(index.Name, index);
            return new CompletedTask();
        }
    }
}
