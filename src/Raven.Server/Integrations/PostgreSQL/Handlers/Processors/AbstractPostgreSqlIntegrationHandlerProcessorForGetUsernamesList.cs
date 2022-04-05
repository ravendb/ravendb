using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal abstract class AbstractPostgreSqlIntegrationHandlerProcessorForGetUsernamesList<TRequestHandler, TOperationContext> : AbstractPostgreSqlIntegrationHandlerProcessor<TRequestHandler, TOperationContext>
    where TRequestHandler : RequestHandler
    where TOperationContext : JsonOperationContext
{
    protected AbstractPostgreSqlIntegrationHandlerProcessorForGetUsernamesList([NotNull] TRequestHandler requestHandler, [NotNull] JsonContextPoolBase<TOperationContext> contextPool)
        : base(requestHandler, contextPool)
    {
    }

    protected abstract string GetDatabaseName();

    public override async ValueTask ExecuteAsync()
    {
        AssertCanUsePostgreSqlIntegration();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            DatabaseRecord databaseRecord;

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
                databaseRecord = RequestHandler.ServerStore.Cluster.ReadDatabase(transactionOperationContext, GetDatabaseName(), out long index);

            var usernames = new List<PostgreSqlUsername>();

            var users = databaseRecord?.Integrations?.PostgreSql?.Authentication?.Users;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                if (users != null)
                {
                    foreach (var user in users)
                    {
                        var username = new PostgreSqlUsername { Username = user.Username };
                        usernames.Add(username);
                    }
                }

                var dto = new PostgreSqlUsernames { Users = usernames };

                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(dto);
                writer.WriteObject(context.ReadObject(djv, "PostgreSqlUsernames"));
            }
        }
    }
}
