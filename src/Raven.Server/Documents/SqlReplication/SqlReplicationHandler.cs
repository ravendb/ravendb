// -----------------------------------------------------------------------
//  <copyright file="SqlReplicationHandler.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.SqlReplication
{
    public class SqlReplicationHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/sql-replication/stats", "GET", "/databases/{databaseName:string}/sql-replication/stats")]
        public Task GetStats()
        {
            throw new NotImplementedException();

            /*
               [HttpGet]
                    [RavenRoute("debug/sql-replication-stats")]
                    [RavenRoute("databases/{databaseName}/debug/sql-replication-stats")]
                    public HttpResponseMessage SqlReplicationStats()
                    {
                        var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
                        if (task == null)
                            return GetMessageWithObject(new
                            {
                                Error = "SQL Replication bundle is not installed"
                            }, HttpStatusCode.NotFound);


                        //var metrics = task.SqlReplicationMetricsCounters.ToDictionary(x => x.Key, x => x.Value.ToSqlReplicationMetricsData());

                        var statisticsAndMetrics = task.GetConfiguredReplicationDestinations().Select(x =>
                        {
                            SqlReplicationStatistics stats;
                            task.Statistics.TryGetValue(x.Name, out stats);
                            var metrics = task.GetSqlReplicationMetricsManager(x).ToSqlReplicationMetricsData();
                            return new
                            {
                                x.Name,
                                Statistics = stats,
                                Metrics = metrics
                            };
                        });
                        return GetMessageWithObject(statisticsAndMetrics);
                    }
            */

            /*
             [HttpGet]
                    [RavenRoute("studio-tasks/get-sql-replication-stats")]
                    [RavenRoute("databases/{databaseName}/studio-tasks/get-sql-replication-stats")]
                    public HttpResponseMessage GetSQLReplicationStats(string sqlReplicationName)
                    {
                        new SqlConnection()
                        var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
                        if (task == null)
                            return GetMessageWithObject(new
                            {
                                Error = "SQL Replication bundle is not installed"
                            }, HttpStatusCode.NotFound);

                        var matchingStats = task.Statistics.FirstOrDefault(x => x.Key == sqlReplicationName);

                        if (matchingStats.Key != null)
                        {
                            return GetMessageWithObject(task.Statistics.FirstOrDefault(x => x.Key == sqlReplicationName));
                        }
                        return GetEmptyMessage(HttpStatusCode.NotFound);
                    }
            */
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
        public Task PostReset()
        {
            /*
            var task = Database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
            if (task == null)
                return GetMessageWithObjectAsTask(new
                {
                    Error = "SQL Replication bundle is not installed"
                }, HttpStatusCode.NotFound);
            SqlReplicationStatistics stats;
            task.Statistics.TryRemove(sqlReplicationName, out stats);
            var jsonDocument = Database.Documents.Get(SqlReplicationTask.RavenSqlReplicationStatus, null);
            if (jsonDocument != null)
            {
                var replicationStatus = jsonDocument.DataAsJson.JsonDeserialization<SqlReplicationStatus>();
                replicationStatus.LastReplicatedEtags.RemoveAll(x => x.Name == sqlReplicationName);
                
                Database.Documents.Put(SqlReplicationTask.RavenSqlReplicationStatus, null, RavenJObject.FromObject(replicationStatus), new RavenJObject(), null);
            }

            return GetEmptyMessageAsTask(HttpStatusCode.NoContent);
            */

            throw new NotImplementedException();
        }
    }
}