// -----------------------------------------------------------------------
//  <copyright file="DebugInfoProvider.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.IO.Compression;
using System.Linq;

using Raven.Abstractions.Data;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Replication;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Bundles.Replication.Utils;
using Raven.Database.Bundles.SqlReplication;
using Raven.Database.Config;
using Raven.Database.Extensions;
using Raven.Database.Indexing;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;
using Raven.Database.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

namespace Raven.Database.Util
{
    public static class DebugInfoProvider
    {
        const CompressionLevel compressionLevel = CompressionLevel.Optimal;

        public static void CreateInfoPackageForDatabase(ZipArchive package, DocumentDatabase database, RequestManager requestManager, string zipEntryPrefix = null)
        {
            zipEntryPrefix = zipEntryPrefix ?? string.Empty;

            var databaseName = database.Name;
            if (string.IsNullOrWhiteSpace(databaseName))
                databaseName = Constants.SystemDatabase;

            var jsonSerializer = new JsonSerializer { Formatting = Formatting.Indented };
            jsonSerializer.Converters.Add(new EtagJsonConverter());

            if (database.StartupTasks.OfType<ReplicationTask>().Any())
            {
                var replication = package.CreateEntry(zipEntryPrefix + "replication.json", compressionLevel);

                using (var statsStream = replication.Open())
                using (var streamWriter = new StreamWriter(statsStream))
                {
                    jsonSerializer.Serialize(streamWriter, ReplicationUtils.GetReplicationInformation(database));
                    streamWriter.Flush();
                }
            }

            var sqlReplicationTask = database.StartupTasks.OfType<SqlReplicationTask>().FirstOrDefault();
            if (sqlReplicationTask != null)
            {
                var replication = package.CreateEntry(zipEntryPrefix + "sql_replication.json", compressionLevel);

                using (var statsStream = replication.Open())
                using (var streamWriter = new StreamWriter(statsStream))
                {
                    jsonSerializer.Serialize(streamWriter, sqlReplicationTask.Statistics);
                    streamWriter.Flush();
                }
            }

            var stats = package.CreateEntry(zipEntryPrefix + "stats.json", compressionLevel);

            using (var statsStream = stats.Open())
            using (var streamWriter = new StreamWriter(statsStream))
            {
                jsonSerializer.Serialize(streamWriter, database.Statistics);
                streamWriter.Flush();
            }

            var metrics = package.CreateEntry(zipEntryPrefix + "metrics.json", compressionLevel);

            using (var metricsStream = metrics.Open())
            using (var streamWriter = new StreamWriter(metricsStream))
            {
                jsonSerializer.Serialize(streamWriter, database.CreateMetrics());
                streamWriter.Flush();
            }

            var logs = package.CreateEntry(zipEntryPrefix + "logs.csv", compressionLevel);

            using (var logsStream = logs.Open())
            using (var streamWriter = new StreamWriter(logsStream))
            {
                var target = LogManager.GetTarget<DatabaseMemoryTarget>();

                if (target == null) streamWriter.WriteLine("DatabaseMemoryTarget was not registered in the log manager, logs are not available");
                else
                {
                    var boundedMemoryTarget = target[databaseName];
                    var log = boundedMemoryTarget.GeneralLog;

                    streamWriter.WriteLine("time,logger,level,message,exception");

                    foreach (var logEvent in log)
                    {
                        streamWriter.WriteLine("{0:O},{1},{2},{3},{4}", logEvent.TimeStamp, logEvent.LoggerName, logEvent.Level, logEvent.FormattedMessage, logEvent.Exception);
                    }
                }

                streamWriter.Flush();
            }

            var config = package.CreateEntry(zipEntryPrefix + "config.json", compressionLevel);

            using (var configStream = config.Open())
            using (var streamWriter = new StreamWriter(configStream))
            using (var jsonWriter = new JsonTextWriter(streamWriter))
            {
                GetConfigForDebug(database).WriteTo(jsonWriter, new EtagJsonConverter());
                jsonWriter.Flush();
            }

            var currentlyIndexing = package.CreateEntry(zipEntryPrefix + "currently-indexing.json", compressionLevel);

            using (var currentlyIndexingStream = currentlyIndexing.Open())
            using (var streamWriter = new StreamWriter(currentlyIndexingStream))
            {
                jsonSerializer.Serialize(streamWriter, GetCurrentlyIndexingForDebug(database));
                streamWriter.Flush();
            }

            var queries = package.CreateEntry(zipEntryPrefix + "queries.json", compressionLevel);

            using (var queriesStream = queries.Open())
            using (var streamWriter = new StreamWriter(queriesStream))
            {
                jsonSerializer.Serialize(streamWriter, database.WorkContext.CurrentlyRunningQueries);
                streamWriter.Flush();
            }

            var prefetchStatus = package.CreateEntry(zipEntryPrefix + "prefetch-status.json", compressionLevel);

            using (var prefetchStatusStream = prefetchStatus.Open())
            using (var streamWriter = new StreamWriter(prefetchStatusStream))
            {
                jsonSerializer.Serialize(streamWriter, GetPrefetchingQueueStatusForDebug(database));
                streamWriter.Flush();
            }

            var requestTracking = package.CreateEntry(zipEntryPrefix + "request-tracking.json", compressionLevel);

            using (var requestTrackingStream = requestTracking.Open())
            using (var streamWriter = new StreamWriter(requestTrackingStream))
            {
                jsonSerializer.Serialize(streamWriter, GetRequestTrackingForDebug(requestManager, databaseName));
                streamWriter.Flush();
            }

            var tasks = package.CreateEntry(zipEntryPrefix + "tasks.json", compressionLevel);

            using (var tasksStream = tasks.Open())
            using (var streamWriter = new StreamWriter(tasksStream))
            {
                jsonSerializer.Serialize(streamWriter, GetTasksForDebug(database));
                streamWriter.Flush();
            }

			var systemUtilization = package.CreateEntry(zipEntryPrefix + "system-utilization.json", compressionLevel);

			using (var systemUtilizationStream = systemUtilization.Open())
			using (var streamWriter = new StreamWriter(systemUtilizationStream))
			{
				var cpuCounter = new PerformanceCounter("Processor", "% Processor Time", "_Total", true);

				long totalPhysicalMemory = -1;
				long availableMemory = -1;
				float currentCpuUsage = -1;

				try
				{
					totalPhysicalMemory = MemoryStatistics.TotalPhysicalMemory;
					availableMemory = MemoryStatistics.AvailableMemory;
					
					cpuCounter.NextValue();
					System.Threading.Thread.Sleep(1000); // wait a second to get a valid reading
					currentCpuUsage = cpuCounter.NextValue();
				}
				catch (Exception)
				{
				}

				jsonSerializer.Serialize(streamWriter, new
				{
					TotalPhysicalMemory = string.Format("{0:#,#.##;;0} MB", totalPhysicalMemory),
					AvailableMemory = string.Format("{0:#,#.##;;0} MB", availableMemory),
					CurrentCpuUsage = string.Format("{0} %", currentCpuUsage)
				});

				streamWriter.Flush();
			}
        }

        internal static object GetRequestTrackingForDebug(RequestManager requestManager, string databaseName)
        {
            return requestManager.GetRecentRequests(databaseName).Select(x => new
            {
                Uri = x.RequestUri,
                Method = x.HttpMethod,
                StatusCode = x.ResponseStatusCode,
                RequestHeaders = x.Headers.AllKeys.Select(k => new { Name = k, Values = x.Headers.GetValues(k) }),
                ExecutionTime = string.Format("{0} ms", x.Stopwatch.ElapsedMilliseconds),
                AdditionalInfo = x.CustomInfo ?? string.Empty
            });
        }

        internal static RavenJObject GetConfigForDebug(DocumentDatabase database)
        {
            var cfg = RavenJObject.FromObject(database.Configuration);
            cfg["OAuthTokenKey"] = "<not shown>";
            var changesAllowed = database.Configuration.Settings["Raven/Versioning/ChangesToRevisionsAllowed"];

            if (string.IsNullOrWhiteSpace(changesAllowed) == false)
                cfg["Raven/Versioning/ChangesToRevisionsAllowed"] = changesAllowed;

            return cfg;
        }

        internal static IList<TaskMetadata> GetTasksForDebug(DocumentDatabase database)
        {
            IList<TaskMetadata> tasks = null;
            database.TransactionalStorage.Batch(accessor =>
            {
                tasks = accessor.Tasks
                    .GetPendingTasksForDebug()
                    .ToList();
            });

            foreach (var taskMetadata in tasks)
            {
                var indexInstance = database.IndexStorage.GetIndexInstance(taskMetadata.IndexId);
                if (indexInstance != null)
                    taskMetadata.IndexName = indexInstance.PublicName;
            }
            return tasks;
        }

        internal static object GetCurrentlyIndexingForDebug(DocumentDatabase database)
        {
            var indexingWork = database.IndexingExecuter.GetCurrentlyProcessingIndexes();
            var reduceWork = database.ReducingExecuter.GetCurrentlyProcessingIndexes();

            var uniqueIndexesBeingProcessed = indexingWork.Union(reduceWork).Distinct(new Index.IndexByIdEqualityComparer()).ToList();

            return new
            {
                NumberOfCurrentlyWorkingIndexes = uniqueIndexesBeingProcessed.Count,
                Indexes = uniqueIndexesBeingProcessed.Select(x => new
                {
                    IndexName = x.PublicName,
                    IsMapReduce = x.IsMapReduce,
                    CurrentOperations = x.GetCurrentIndexingPerformance().Select(p => new { p.Operation, NumberOfProcessingItems = p.InputCount }),
                    Priority = x.Priority,
                    OverallIndexingRate = x.GetIndexingPerformance().Where(ip => ip.Duration != TimeSpan.Zero).GroupBy(y => y.Operation).Select(g => new
                    {
                        Operation = g.Key,
                        Rate = string.Format("{0:0.0000} ms/doc", g.Sum(z => z.Duration.TotalMilliseconds) / g.Sum(z => z.InputCount))
                    })
                })
            };
        }

        internal static object GetPrefetchingQueueStatusForDebug(DocumentDatabase database)
        {
            var prefetcherDocs = database.IndexingExecuter.PrefetchingBehavior.DebugGetDocumentsInPrefetchingQueue().ToArray();
            var compareToCollection = new Dictionary<Etag, int>();

            for (int i = 1; i < prefetcherDocs.Length; i++)
                compareToCollection.Add(prefetcherDocs[i - 1].Etag, prefetcherDocs[i].Etag.CompareTo(prefetcherDocs[i - 1].Etag));

            if (compareToCollection.Any(x => x.Value < 0))
            {
                return new
                {
                    HasCorrectlyOrderedEtags = true,
                    EtagsWithKeys = prefetcherDocs.ToDictionary(x => x.Etag, x => x.Key)
                };
            }

            return new
            {
                HasCorrectlyOrderedEtags = false,
                IncorrectlyOrderedEtags = compareToCollection.Where(x => x.Value < 0),
                EtagsWithKeys = prefetcherDocs.ToDictionary(x => x.Etag, x => x.Key)
            };
        }
    }
}