// -----------------------------------------------------------------------
//  <copyright file="SqlEtlHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.SQL.Handlers.Processors;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Handlers
{
    public class SqlEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/sql/test-connection", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task TestConnection()
        {
            using (var processor = new SqlEtlHandlerProcessorForTestConnection<DocumentsOperationContext>(this))
                await processor.ExecuteAsync();
        }

        [RavenAction("/databases/*/admin/etl/sql/test", "POST", AuthorizationStatus.DatabaseAdmin)]
        public async Task Test()
        {
            using (var processor = new SqlEtlHandlerProcessorForTest(this))
                await processor.ExecuteAsync();
        }
    }
}
