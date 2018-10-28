using System.Threading.Tasks;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;

namespace Raven.Server.Smuggler.Migration
{
    public abstract class AbstractMigrator
    {
        protected readonly MigratorOptions Options;

        protected AbstractMigrator(MigratorOptions options)
        {
            Options = options;
        }

        public abstract Task Execute();

        protected async Task SaveLastOperationState(BlittableJsonReaderObject blittable)
        {
            using (var cmd = new MergedPutCommand(blittable, Options.MigrationStateKey, null, Options.Database))
            {
                await Options.Database.TxMerger.Enqueue(cmd);
            }
        }
    }
}
