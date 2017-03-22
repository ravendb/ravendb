// -----------------------------------------------------------------------
//  <copyright file="SqlEtlHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Server.Documents.ETL.Providers.SQL.RelationalWriters;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.ETL.Providers.SQL.Handlers
{
    public class SqlEtlHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/etl/sql/test-sql-connection", "GET", "/databases/{databaseName:string}/etl/sql/test-sql-connection?factoryName={factoryName:string}&connectionString{connectionString:string}")]
        public Task GetTestSqlConnection()
        {
            try
            {
                var factoryName = GetStringQueryString("factoryName");
                var connectionString = GetStringQueryString("connectionString");
                RelationalDatabaseWriter.TestConnection(factoryName, connectionString);
                NoContentStatus();
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest; // Bad Request

                if (Logger.IsInfoEnabled)
                    Logger.Info("Error occured during sql replication connection test", ex);

                JsonOperationContext context;
                using (ContextPool.AllocateOperationContext(out context))
                {
                    using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                    {
                        context.Write(writer, new DynamicJsonValue
                        {
                            ["Error"] = "Connection failed",
                            ["Exception"] = ex.ToString(),
                        });
                    }
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/etl/sql/simulate", "POST", "/databases/{databaseName:string}/etl/sql/simulate")]
        public Task PostSimulateSqlReplication()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var dbDoc = context.ReadForMemory(RequestBodyStream(), "SimulateSqlReplicationResult");
                var simulateSqlReplication = JsonDeserializationServer.SimulateSqlReplication(dbDoc);
                var result = SqlEtl.SimulateSqlEtl(simulateSqlReplication, Database, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }

            return Task.CompletedTask;
        }
    }
}