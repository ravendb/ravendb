using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions.Indexing;
using Raven.Abstractions.Util;
using Raven.Smuggler.Database;

namespace Raven.Database.Smuggler.Embedded
{
    public class DatabaseSmugglerEmbeddedTransformerActions : IDatabaseSmugglerTransformerActions
    {
        private readonly DocumentDatabase _database;

        public DatabaseSmugglerEmbeddedTransformerActions(DocumentDatabase database)
        {
            _database = database;
        }

        public void Dispose()
        {
        }

        public Task WriteTransformerAsync(TransformerDefinition transformer, CancellationToken cancellationToken)
        {
            _database.Transformers.PutTransform(transformer.Name, transformer);
            return new CompletedTask();
        }
    }
}
