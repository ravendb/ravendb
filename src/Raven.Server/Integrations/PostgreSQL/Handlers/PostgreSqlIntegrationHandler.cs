using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Integrations.PostgreSQL.Handlers.Processors;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Handlers
{
    public class PostgreSqlIntegrationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/integrations/postgresql/server/status", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetServerStatus()
        {
            using (var processor = new PostgreSqlIntegrationHandlerProcessorForGetServerStatus<DocumentsOperationContext>(this, ContextPool))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/users", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetUsernamesList()
        {
            using (var processor = new PostgreSqlIntegrationHandlerProcessorForGetUsernamesList(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/user", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddUser()
        {
            AssertCanUsePostgreSqlIntegration();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var newUserRequest = await context.ReadForMemoryAsync(RequestBodyStream(), "PostgreSQLNewUser");
                var dto = JsonDeserializationServer.PostgreSqlUser(newUserRequest);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    if (string.IsNullOrEmpty(dto.Username))
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        context.Write(writer, new DynamicJsonValue { ["Error"] = "Username is null or empty." });
                        return;
                    }

                    if (string.IsNullOrEmpty(dto.Password))
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        context.Write(writer, new DynamicJsonValue { ["Error"] = "Password is null or empty." });
                        return;
                    }

                    DatabaseRecord databaseRecord;

                    using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                    using (transactionOperationContext.OpenReadTransaction())
                        databaseRecord = Database.ServerStore.Cluster.ReadDatabase(transactionOperationContext, Database.Name, out long index);

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
                        return;
                    }

                    users.Add(newUser);

                    using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                        await DatabaseConfigurations(ServerStore.ModifyPostgreSqlConfiguration, transactionOperationContext, RaftIdGenerator.DontCareId, config);
                }
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/user", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteUser()
        {
            AssertCanUsePostgreSqlIntegration();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var username = GetQueryStringValueAndAssertIfSingleAndNotEmpty("username");

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    if (string.IsNullOrEmpty(username))
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        context.Write(writer, new DynamicJsonValue { ["Error"] = "Username is null or empty." });
                        return;
                    }

                    DatabaseRecord databaseRecord;

                    using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                    using (transactionOperationContext.OpenReadTransaction())
                        databaseRecord = Database.ServerStore.Cluster.ReadDatabase(transactionOperationContext, Database.Name, out long index);

                    var config = databaseRecord.Integrations?.PostgreSql;

                    if (config == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        context.Write(writer, new DynamicJsonValue { ["Error"] = "Unable to get usernames from database record" });
                        return;
                    }

                    var users = config.Authentication.Users;

                    var userToDelete = users.SingleOrDefault(x => x.Username == username);

                    if (userToDelete == null)
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        context.Write(writer, new DynamicJsonValue { ["Error"] = $"{username} username does not exist." });
                        return;
                    }

                    users.Remove(userToDelete);

                    using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                        await DatabaseConfigurations(ServerStore.ModifyPostgreSqlConfiguration, transactionOperationContext, RaftIdGenerator.DontCareId, config);
                }
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigPostgreSql()
        {
            AssertCanUsePostgreSqlIntegration();

            await DatabaseConfigurations(ServerStore.ModifyPostgreSqlConfiguration, "read-postgresql-config", GetRaftRequestIdFromQuery());
        }


    }
}
