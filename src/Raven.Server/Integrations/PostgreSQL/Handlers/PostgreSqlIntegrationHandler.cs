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
            using (var processor = new PostgreSqlIntegrationHandlerProcessorForAddUser(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/user", "DELETE", AuthorizationStatus.DatabaseAdmin)]
        public async Task DeleteUser()
        {
            using (var processor = new PostgreSqlIntegrationHandlerProcessorForDeleteUser(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/integrations/postgresql/config", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task ConfigPostgreSql()
        {
            AssertCanUsePostgreSqlIntegration();

            await DatabaseConfigurations(ServerStore.ModifyPostgreSqlConfiguration, "read-postgresql-config", GetRaftRequestIdFromQuery());
        }
    }
}
