using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.ServerWide;
using Raven.Server.Documents;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Integrations.PostgreSQL.Handlers
{
    public class PostgreSqlIntegrationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/integrations/postgresql/users", "GET", AuthorizationStatus.ValidUser, EndpointType.Read)]
        public async Task GetUsernamesList()
        {
            //todo pfyasu: delete lines below - debug only
            var usernames = new List<User>
            {
                new() { Username = "User1" },
                new() { Username = "User2" }
            };

            var dto = new PostgreSqlUsernames { Users = usernames };

            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                DatabaseRecord databaseRecord;

                using (Database.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext transactionOperationContext))
                using (transactionOperationContext.OpenReadTransaction())
                    databaseRecord = Database.ServerStore.Cluster.ReadDatabase(transactionOperationContext, Database.Name, out long index);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(dto);
                    writer.WriteObject(context.ReadObject(djv, "PostgreSQLUsernamesList"));
                }
            }
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/user", "PUT", AuthorizationStatus.DatabaseAdmin)]
        public async Task AddUser()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var newUserRequest = await context.ReadForMemoryAsync(RequestBodyStream(), "PostgreSQLNewUser");
                var newUserDto = JsonDeserializationServer.PostgreSqlUser(newUserRequest);

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    //todo pfyasu check user with username from dtos already exists in db
                    if (string.IsNullOrEmpty(newUserDto.Username))
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        context.Write(writer, new DynamicJsonValue { ["Error"] = "Username is null or empty." });
                        return;
                    }

                    if (string.IsNullOrEmpty(newUserDto.Password))
                    {
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;
                        context.Write(writer, new DynamicJsonValue { ["Error"] = "Password is null or empty." });
                        return;
                    }

                    //todo pfyasu delete line below - debug only
                    System.Console.WriteLine($"New user (username: {newUserDto.Username} | password: {newUserDto.Password}) has been added");
                }
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/user", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteUser()
        {
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

                    //todo pfyasu delete line below - debug only
                    System.Console.WriteLine($"{username} user has been deleted");
                }
            }

            NoContentStatus();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigPostgreSql()
        {
            await DatabaseConfigurations(ServerStore.ModifyPostgreSqlConfiguration, "read-postgresql-config", GetRaftRequestIdFromQuery());
        }
    }
}
