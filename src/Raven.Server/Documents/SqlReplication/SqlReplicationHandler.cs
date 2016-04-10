// -----------------------------------------------------------------------
//  <copyright file="SqlReplicationHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/sql-replication/stats", "GET", "/databases/{databaseName:string}/sql-replication/stats?name={sqlReplicationName:string}")]
        public Task GetStats()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count == 0)
                throw new ArgumentException("Query string \'name\' is mandatory, but wasn\'t specified");
            var name = names[0];
            var replication = Database.SqlReplicationLoader.Replications.FirstOrDefault(r => r.Name == name);

            if (replication == null)
            {
                HttpContext.Response.StatusCode = 404;
                return Task.CompletedTask;
            }

            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, replication.Statistics.ToBlittable());
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/sql-replication/debug/stats", "GET", "/databases/{databaseName:string}/sql-replication/debug/stats")]
        public Task GetDebugStats()
        {
            JsonOperationContext context;
            using (ServerStore.ContextPool.AllocateOperationContext(out context))
            {
                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartArray();
                    bool first = true;
                    foreach (var replication in Database.SqlReplicationLoader.Replications)
                    {
                        if (first == false)
                            writer.WriteComma();
                        else
                            first = false;

                        var json = new DynamicJsonValue
                        {
                            ["Name"] = replication.Name,
                            ["Statistics"] = replication.Statistics.ToBlittable(),
                            ["Metrics"] = new DynamicJsonValue
                            {
                                /*TODO: Metrics*/
                            },
                        };
                        context.Write(writer, json);
                    }
                    writer.WriteEndArray();
                }
            }
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/sql-replication/debug/pref", "GET", "/databases/{databaseName:string}/sql-replication/debug/pref")]
        public Task GetDebugPref()
        {
/* TODO: Implement*/
            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/sql-replication/test-sql-connection", "GET", "/databases/{databaseName:string}/sql-replication/test-sql-connection?factoryName={factoryName:string}&connectionString{connectionString:string}")]
        public Task GetTestSqlConnection()
        {
            try
            {
                var factoryName = GetStringQueryString("factoryName", true);
                var connectionString = GetStringQueryString("connectionString", true);
                RelationalDatabaseWriter.TestConnection(factoryName, connectionString);
                HttpContext.Response.StatusCode = 204; // No Content
            }
            catch (Exception ex)
            {
                HttpContext.Response.StatusCode = 400; // Bad Request

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

        [RavenAction("/databases/*/sql-replication/simulate", "POST", "/databases/{databaseName:string}/sql-replication/simulate")]
        public Task PostSimulateSqlReplication()
        {
            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            {
                context.OpenReadTransaction();

                var dbDoc = context.ReadForMemory(RequestBodyStream(), "SimulateSqlReplicationResult");
                var simulateSqlReplication = JsonDeserialization.SimulateSqlReplication(dbDoc);
                var result = Database.SqlReplicationLoader.SimulateSqlReplicationSqlQueries(simulateSqlReplication, context);

                using (var writer = new BlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    context.Write(writer, result);
                }
            }

            return Task.CompletedTask;
        }

        [RavenAction("/databases/*/sql-replication/reset", "POST", "/databases/{databaseName:string}/sql-replication/reset?name={sqlReplicationName:string}")]
        public Task PostResetSqlReplication()
        {
            var names = HttpContext.Request.Query["name"];
            if (names.Count == 0)
                throw new ArgumentException("Query string \'name\' is mandatory, but wasn\'t specified");
            var name = names[0];
            var replication = Database.SqlReplicationLoader.Replications.FirstOrDefault(r => r.Name == name);

            if (replication == null)
            {
                HttpContext.Response.StatusCode = 404;
                return Task.CompletedTask;
            }

            DocumentsOperationContext context;
            using (Database.DocumentsStorage.ContextPool.AllocateOperationContext(out context))
            using (var tx = context.OpenWriteTransaction())
            {
                Database.DocumentsStorage.Delete(context, Constants.SqlReplication.RavenSqlReplicationStatusPrefix + name, null);
                tx.Commit();
            }

            HttpContext.Response.StatusCode = 204;  // NoContent
            return Task.CompletedTask;
        }
    }
}