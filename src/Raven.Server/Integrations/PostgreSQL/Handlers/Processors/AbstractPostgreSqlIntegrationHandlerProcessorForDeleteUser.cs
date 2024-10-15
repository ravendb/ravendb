using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal abstract class AbstractPostgreSqlIntegrationHandlerProcessorForDeleteUser<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<PostgreSqlConfiguration, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractPostgreSqlIntegrationHandlerProcessorForDeleteUser([NotNull] TRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override Task<(long Index, object Result)> OnUpdateConfiguration(TransactionOperationContext context, PostgreSqlConfiguration configuration, string raftRequestId)
    {
        return RequestHandler.ServerStore.ModifyPostgreSqlConfiguration(context, RequestHandler.DatabaseName, configuration, raftRequestId);
    }

    protected override ValueTask AssertCanExecuteAsync()
    {
        AbstractPostgreSqlIntegrationHandlerProcessor<TRequestHandler, TOperationContext>.AssertCanUsePostgreSqlIntegration(RequestHandler);

        return base.AssertCanExecuteAsync();
    }

    protected override ValueTask<PostgreSqlConfiguration> GetConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        var username = RequestHandler.GetQueryStringValueAndAssertIfSingleAndNotEmpty("username");

        if (string.IsNullOrEmpty(username))
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Write(writer, new DynamicJsonValue { ["Error"] = "Username is null or empty." });
            return ValueTask.FromResult<PostgreSqlConfiguration>(null); // handled
        }

        DatabaseRecord databaseRecord;

        using (context.OpenReadTransaction())
            databaseRecord = RequestHandler.ServerStore.Cluster.ReadDatabase(context, RequestHandler.DatabaseName, out _);

        var config = databaseRecord.Integrations?.PostgreSql;

        if (config == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Write(writer, new DynamicJsonValue { ["Error"] = "Unable to get usernames from database record" });
            return ValueTask.FromResult<PostgreSqlConfiguration>(null); // handled
        }

        var users = config.Authentication.Users;

        var userToDelete = users.SingleOrDefault(x => x.Username == username);

        if (userToDelete == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Write(writer, new DynamicJsonValue { ["Error"] = $"{username} username does not exist." });
            return ValueTask.FromResult<PostgreSqlConfiguration>(null); // handled
        }

        users.Remove(userToDelete);

        if (LoggingSource.AuditLog.IsInfoEnabled)
        {
            RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "DELETE", $"User '{userToDelete.Username}' in Postgres integration");
        }

        return ValueTask.FromResult(config);
    }
}
