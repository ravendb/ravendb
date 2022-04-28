using System.Collections.Generic;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal abstract class AbstractPostgreSqlIntegrationHandlerProcessorForGetUsernamesList<TRequestHandler, TOperationContext> : AbstractPostgreSqlIntegrationHandlerProcessor<TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext 
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractPostgreSqlIntegrationHandlerProcessorForGetUsernamesList([NotNull] TRequestHandler requestHandler)
        : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        AssertCanUsePostgreSqlIntegration(RequestHandler);

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        {
            DatabaseRecord databaseRecord;

            using (RequestHandler.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
            using (transactionOperationContext.OpenReadTransaction())
                databaseRecord = RequestHandler.ServerStore.Cluster.ReadDatabase(transactionOperationContext, RequestHandler.DatabaseName, out long index);

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
