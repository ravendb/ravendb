// -----------------------------------------------------------------------
//  <copyright file="SnowflakeEtlHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake.Handlers.Processors;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;

namespace Raven.Server.Documents.ETL.Providers.RelationalDatabase.Snowflake.Handlers;

public sealed class SnowflakeEtlHandler : DatabaseRequestHandler
{
    [RavenAction("/databases/*/admin/etl/snowflake/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task TestConnection()
    {
        using (var processor = new SnowflakeEtlHandlerProcessorForTestConnection<DocumentsOperationContext>(this))
            await processor.ExecuteAsync();
    }

    [RavenAction("/databases/*/admin/etl/snowflake/test", "POST", AuthorizationStatus.DatabaseAdmin)]
    public async Task Test()
    {
        using (var processor = new SnowflakeEtlHandlerProcessorForTest(this))
            await processor.ExecuteAsync();
    }
}
