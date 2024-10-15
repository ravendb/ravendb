using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors.Databases;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Integrations.PostgreSQL.Handlers.Processors;

internal abstract class AbstractPostgreSqlIntegrationHandlerProcessorForAddUser<TRequestHandler, TOperationContext> : AbstractHandlerProcessorForUpdateDatabaseConfiguration<PostgreSqlConfiguration, TRequestHandler, TOperationContext>
    where TOperationContext : JsonOperationContext
    where TRequestHandler : AbstractDatabaseRequestHandler<TOperationContext>
{
    protected AbstractPostgreSqlIntegrationHandlerProcessorForAddUser([NotNull] TRequestHandler requestHandler) : base(requestHandler)
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

    protected override async ValueTask<PostgreSqlConfiguration> GetConfigurationAsync(TransactionOperationContext context, AsyncBlittableJsonTextWriter writer)
    {
        var json = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), GetType().Name);
        var dto = JsonDeserializationServer.PostgreSqlUser(json);

        if (string.IsNullOrEmpty(dto.Username))
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Write(writer, new DynamicJsonValue { ["Error"] = "Username is null or empty." });
            return null; // handled
        }

        if (string.IsNullOrEmpty(dto.Password))
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Write(writer, new DynamicJsonValue { ["Error"] = "Password is null or empty." });
            return null; // handled
        }

        DatabaseRecord databaseRecord;

        using (context.OpenReadTransaction())
            databaseRecord = RequestHandler.ServerStore.Cluster.ReadDatabase(context, RequestHandler.DatabaseName, out _);

        var newUser = new PostgreSqlUser
        {
            Username = dto.Username,
            Password = dto.Password
        };

        var config = databaseRecord.Integrations?.PostgreSql;

        if (config == null)
        {
            config = new PostgreSqlConfiguration()
            {
                Authentication = new PostgreSqlAuthenticationConfiguration()
                {
                    Users = new List<PostgreSqlUser>()
                }
            };
        }

        var users = config.Authentication.Users;

        if (users.Any(x => x.Username.Equals(dto.Username, StringComparison.OrdinalIgnoreCase)))
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
            context.Write(writer, new DynamicJsonValue { ["Error"] = $"{dto.Username} username already exists." });
            return null;
        }

        users.Add(newUser);

        if (LoggingSource.AuditLog.IsInfoEnabled)
        {
            RequestHandler.LogAuditFor(RequestHandler.DatabaseName, "PUT", $"User '{newUser.Username}' in Postgres integration");
        }

        return config;
    }
}
