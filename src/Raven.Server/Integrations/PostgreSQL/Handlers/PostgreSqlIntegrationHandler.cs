using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Exceptions.Commercial;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations.Integrations.PostgreSQL;
using Raven.Client.Util;
using Raven.Server.Config;
using Raven.Server.Config.Categories;
using Raven.Server.Documents;
using Raven.Server.Exceptions;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Utils.Features;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Logging;

namespace Raven.Server.Integrations.PostgreSQL.Handlers
{
    public class PostgreSqlIntegrationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/integrations/postgresql/server/status", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetServerStatus()
        {
            AssertCanUsePostgreSqlIntegration();

            using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
            {
                var dto = new PostgreSqlServerStatus { Active = Server.PostgresServer.Active };

                var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(dto);
                writer.WriteObject(context.ReadObject(djv, "PostgreSqlServerStatus"));
            }
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/users", "GET", AuthorizationStatus.DatabaseAdmin)]
        public async Task GetUsernamesList()
        {
            AssertCanUsePostgreSqlIntegration();

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                DatabaseRecord databaseRecord;

                using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                using (transactionOperationContext.OpenReadTransaction())
                    databaseRecord = Database.ServerStore.Cluster.ReadDatabase(transactionOperationContext, Database.Name, out long index);

                var usernames = new List<PostgreSqlUsername>();

                var users = databaseRecord?.Integrations?.PostgreSql?.Authentication?.Users;

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
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

                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        LogAuditFor(Database.Name, "PUT", $"User '{newUser.Username}' in Postgres integration");
                    }
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
                        context.Write(writer, new DynamicJsonValue {["Error"] = "Unable to get usernames from database record"});
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

                    if (LoggingSource.AuditLog.IsInfoEnabled)
                    {
                        LogAuditFor(Database.Name, "DELETE", $"User '{userToDelete.Username}' in Postgres integration");
                    }
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

        private void AssertCanUsePostgreSqlIntegration()
        {
            if (Database.ServerStore.LicenseManager.CanUsePowerBi(false, out _))
                return;

            if (Database.ServerStore.LicenseManager.CanUsePostgreSqlIntegration(withNotification: true))
            {
                ServerStore.FeatureGuardian.Assert(Feature.PostgreSql, () => 
                    $"You have enabled the PostgreSQL integration via '{RavenConfiguration.GetKey(x => x.Integrations.PostgreSql.Enabled)}' configuration but " +
                    "this is an experimental feature and the server does not support experimental features. " +
                    $"Please enable experimental features by changing '{RavenConfiguration.GetKey(x => x.Core.FeaturesAvailability)}' configuration value to '{nameof(FeaturesAvailability.Experimental)}'.");
                return;
            }

            throw new LicenseLimitException("You cannot use this feature because your license doesn't allow neither PostgreSQL integration feature nor Power BI");
        }
    }
}
