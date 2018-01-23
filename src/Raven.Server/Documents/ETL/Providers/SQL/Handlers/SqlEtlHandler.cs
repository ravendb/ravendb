// -----------------------------------------------------------------------
//  <copyright file="SqlEtlHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.IO;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Raven.Server.Web.System;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Handlers
{
    public class SqlEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/admin/etl/sql/test-connection", "POST", AuthorizationStatus.Operator)]
        public Task GetTestSqlConnection()
        {
            try
            {
                var factoryName = GetStringQueryString("factoryName");
                var connectionString = new StreamReader(HttpContext.Request.Body).ReadToEnd();
                RelationalDatabaseWriter.TestConnection(factoryName, connectionString);
                
                DynamicJsonValue result = new DynamicJsonValue
                    {
                        [nameof(NodeConnectionTestResult.Success)] = true,
                    };
                
                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
                
            }
            catch (Exception ex)
            {
                if (Logger.IsInfoEnabled)
                    Logger.Info("Error occurred during sql replication connection test", ex);

                using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
                {
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            [nameof(NodeConnectionTestResult.Success)] = false,
                            [nameof(NodeConnectionTestResult.Error)] = ex.ToString()
                        });
                    }
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/admin/etl/sql/simulate", "POST", AuthorizationStatus.Operator)]
        public Task PostSimulateSqlReplication()
        {
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                context.OpenReadTransaction();

                var dbDoc = context.ReadForMemory(RequestBodyStream(), "SimulateSqlReplicationResult");
                var simulateSqlReplication = JsonDeserializationServer.SimulateSqlReplication(dbDoc);
                var result = SqlEtl.SimulateSqlEtl(simulateSqlReplication, Database, ServerStore, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(result);
                    writer.WriteObject(context.ReadObject(djv, "et/sql/simulate"));
                }
            }

            return Task.CompletedTask;
        }
    }
}
