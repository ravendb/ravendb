using System.Threading.Tasks;
using Raven.Server.Documents.Handlers;
using Sparrow.Json;
using Raven.Server.Routing;

namespace Raven.Server.Smuggler.Migration
{
    public abstract class AbstractMigrator
    {
        protected readonly MigratorOptions Options;
        protected readonly MigratorParameters Parameters;
        protected readonly AuthorizationStatus AuthorizationStatus;

        protected AbstractMigrator(MigratorOptions options, MigratorParameters parameters, AuthorizationStatus authorizationStatus)
        {
            Options = options;
            Parameters = parameters;
            AuthorizationStatus = authorizationStatus;
        }

        public abstract Task Execute();

        protected async Task SaveLastOperationState(BlittableJsonReaderObject blittable)
        {
            using (var cmd = new MergedPutCommand(blittable, Options.MigrationStateKey, null, Parameters.Database))
            {
                await Parameters.Database.TxMerger.Enqueue(cmd);
            }
        }
    }
}
