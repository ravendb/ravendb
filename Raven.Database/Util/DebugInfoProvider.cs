// -----------------------------------------------------------------------
//  <copyright file="DebugInfoProvider.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Bundles.Replication.Tasks;
using Raven.Database.Bundles.Replication.Utils;
using Raven.Database.Bundles.SqlReplication;
using Raven.Database.Config;
using Raven.Database.Indexing;
using Raven.Database.Raft;
using Raven.Database.Raft.Dto;
using Raven.Database.Server.WebApi;
using Raven.Database.Tasks;
using Raven.Imports.Newtonsoft.Json;
using Raven.Json.Linq;

using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Management;
using System.Reflection;
using Raven.Database.Config.Settings;

namespace Raven.Database.Util
{
    public static class DebugInfoProvider
    {
        private const CompressionLevel CompressionLevel = System.IO.Compression.CompressionLevel.Optimal;

        public static void CreateInfoPackageForDatabase(ZipArchive package, DocumentDatabase database, RequestManager requestManager, ClusterManager clusterManager, string zipEntryPrefix = null)
        {
            zipEntryPrefix = zipEntryPrefix ?? string.Empty;

            var databaseName = database.Name;
            if (string.IsNullOrWhiteSpace(databaseName))
                databaseName = Constants.SystemDatabase;

            var jsonSerializer = JsonExtensions.CreateDefaultJsonSerializer();
            jsonSerializer.Formatting=Formatting.Indented;

            if (database.StartupTasks.OfType<ReplicationTask>().Any())
            {
                var replication = package.CreateEntry(zipEntryPrefix + "replication.json", CompressionLevel);

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
                var replication = package.CreateEntry(zipEntryPrefix + "sql_replication.json", CompressionLevel);

                using (var statsStream = replication.Open())
                using (var streamWriter = new StreamWriter(statsStream))
                {
                    jsonSerializer.Serialize(streamWriter, sqlReplicationTask.Statistics);
                    streamWriter.Flush();
                }
            }

            var stats = package.CreateEntry(zipEntryPrefix + "stats.json", CompressionLevel);

            using (var statsStream = stats.Open())
            using (var streamWriter = new StreamWriter(statsStream))
            {
                jsonSerializer.Serialize(streamWriter, database.Statistics);
                streamWriter.Flush();
            }

            var indexingPerformanceStats = package.CreateEntry(zipEntryPrefix + "indexing_performance_stats.json", CompressionLevel);

            using (var statsStream = indexingPerformanceStats.Open())
            using (var streamWriter = new StreamWriter(statsStream))
            {
                jsonSerializer.Serialize(streamWriter, database.IndexingPerformanceStatistics);
                streamWriter.Flush();
            }

            var metrics = package.CreateEntry(zipEntryPrefix + "metrics.json", CompressionLevel);

            using (var metricsStream = metrics.Open())
            using (var streamWriter = new StreamWriter(metricsStream))
            {
                jsonSerializer.Serialize(streamWriter, database.CreateMetrics());
                streamWriter.Flush();
            }

            var logs = package.CreateEntry(zipEntryPrefix + "logs.csv", CompressionLevel);

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

            var config = package.CreateEntry(zipEntryPrefix + "config.json", CompressionLevel);

            using (var configStream = config.Open())
            using (var streamWriter = new StreamWriter(configStream))
            using (var jsonWriter = new JsonTextWriter(streamWriter) { Formatting = Formatting.Indented })
            {
                GetConfigForDebug(database).WriteTo(jsonWriter, new EtagJsonConverter());
                jsonWriter.Flush();
            }

            var indexes = package.CreateEntry(zipEntryPrefix + "indexes.json", CompressionLevel);

            using (var indexesStream = indexes.Open())
            using (var streamWriter = new StreamWriter(indexesStream))
            {
                jsonSerializer.Serialize(streamWriter, database.IndexDefinitionStorage.IndexDefinitions.ToDictionary(x => x.Key, x => x.Value));
                streamWriter.Flush();
            }

            var indexingBatchStats = package.CreateEntry(zipEntryPrefix + "indexing-batch-stats.json", CompressionLevel);

            using (var indexingBatchStatsStream = indexingBatchStats.Open())
            using (var streamWriter = new StreamWriter(indexingBatchStatsStream))
            {
                jsonSerializer.Serialize(streamWriter, database.WorkContext.LastActualIndexingBatchInfo.ToArray());
                streamWriter.Flush();
            }

            var currentlyIndexing = package.CreateEntry(zipEntryPrefix + "currently-indexing.json", CompressionLevel);

            using (var currentlyIndexingStream = currentlyIndexing.Open())
            using (var streamWriter = new StreamWriter(currentlyIndexingStream))
            {
                jsonSerializer.Serialize(streamWriter, GetCurrentlyIndexingForDebug(database));
                streamWriter.Flush();
            }

            var queries = package.CreateEntry(zipEntryPrefix + "queries.json", CompressionLevel);

            using (var queriesStream = queries.Open())
            using (var streamWriter = new StreamWriter(queriesStream))
            {
                jsonSerializer.Serialize(streamWriter, database.WorkContext.CurrentlyRunningQueries);
                streamWriter.Flush();
            }

            var version = package.CreateEntry(zipEntryPrefix + "version.json", CompressionLevel);

            using (var versionStream = version.Open())
            using (var streamWriter = new StreamWriter(versionStream))
            {
                jsonSerializer.Serialize(streamWriter, new
                {
                    DocumentDatabase.ProductVersion,
                    DocumentDatabase.BuildVersion   
                });
                streamWriter.Flush();
            }

            var prefetchStatus = package.CreateEntry(zipEntryPrefix + "prefetch-status.json", CompressionLevel);

            using (var prefetchStatusStream = prefetchStatus.Open())
            using (var streamWriter = new StreamWriter(prefetchStatusStream))
            {
                jsonSerializer.Serialize(streamWriter, GetPrefetchingQueueStatusForDebug(database));
                streamWriter.Flush();
            }

            var requestTracking = package.CreateEntry(zipEntryPrefix + "request-tracking.json", CompressionLevel);

            using (var requestTrackingStream = requestTracking.Open())
            using (var streamWriter = new StreamWriter(requestTrackingStream))
            {
                jsonSerializer.Serialize(streamWriter, GetRequestTrackingForDebug(requestManager, databaseName));
                streamWriter.Flush();
            }

            var tasks = package.CreateEntry(zipEntryPrefix + "tasks.json", CompressionLevel);

            using (var tasksStream = tasks.Open())
            using (var streamWriter = new StreamWriter(tasksStream))
            {
                jsonSerializer.Serialize(streamWriter, GetTasksForDebug(database));
                streamWriter.Flush();
            }

            var expectedRangeWarnings = package.CreateEntry(zipEntryPrefix + "expected-range-warnings.json", CompressionLevel);

            using (var expectedRangeWarningsStream = expectedRangeWarnings.Open())
            using(var streamWriter = new StreamWriter(expectedRangeWarningsStream))
            {
                jsonSerializer.Serialize(streamWriter, GetExpectedRangeWarnings(database));
                streamWriter.Flush();
            }

            var systemUtilization = package.CreateEntry(zipEntryPrefix + "system-utilization.json", CompressionLevel);

            using (var systemUtilizationStream = systemUtilization.Open())
            using (var streamWriter = new StreamWriter(systemUtilizationStream))
            {
                long totalPhysicalMemory = -1;
                long availableMemory = -1;
                object cpuTimes;

                try
                {
                    totalPhysicalMemory = MemoryStatistics.TotalPhysicalMemory;
                    availableMemory = MemoryStatistics.AvailableMemoryInMb;

                    using (var searcher = new ManagementObjectSearcher("select * from Win32_PerfFormattedData_PerfOS_Processor"))
                    {
                        cpuTimes = searcher.Get()
                            .Cast<ManagementObject>()
                            .Select(mo => new
                            {
                                Name = mo["Name"],
                                Usage = string.Format("{0} %", mo["PercentProcessorTime"])
                            }).ToArray();	
                    }
                }
                catch (Exception e)
                {
                    cpuTimes = "Could not get CPU times" + Environment.NewLine + e;
                }

                jsonSerializer.Serialize(streamWriter, new
                {
                    TotalPhysicalMemory = string.Format("{0:#,#.##;;0} MB", totalPhysicalMemory),
                    AvailableMemory = string.Format("{0:#,#.##;;0} MB", availableMemory),
                    CurrentCpuUsage = cpuTimes
                });

                streamWriter.Flush();
            }

            if (clusterManager != null && database.IsSystemDatabase())
            {
                var clusterTopology = package.CreateEntry(zipEntryPrefix + "cluster-topology.json", CompressionLevel);

                using (var stream = clusterTopology.Open())
                using (var streamWriter = new StreamWriter(stream))
                {
                    jsonSerializer.Serialize(streamWriter, clusterManager.GetTopology());
                    streamWriter.Flush();
        }

                var configurationJson = database.Documents.Get(Constants.Cluster.ClusterConfigurationDocumentKey, null);
                if (configurationJson == null)
                    return;

                var configuration = configurationJson.DataAsJson.JsonDeserialization<ClusterConfiguration>();

                var clusterConfiguration = package.CreateEntry(zipEntryPrefix + "cluster-configuration.json", CompressionLevel);

                using (var stream = clusterConfiguration.Open())
                using (var streamWriter = new StreamWriter(stream))
                {
                    jsonSerializer.Serialize(streamWriter, configuration);
                    streamWriter.Flush();
                }
            }
        }

        internal static object GetExpectedRangeWarnings(DocumentDatabase database)
        {
            Dictionary<string, Dictionary<string, ExpectedRange>> rangeWarnings = 
                new Dictionary<string, Dictionary<string, ExpectedRange>>
                {
                    {"config", CreateRangeConfigWarnings(database)},
                    {"indexs",CreateRangeIndexesWarnings(database)}
                };
            return rangeWarnings;
        }

        private static Dictionary<string, ExpectedRange> CreateRangeIndexesWarnings(DocumentDatabase database)
        {
            Dictionary<string, ExpectedRange> rangeWarnings = new Dictionary<string, ExpectedRange>();
            var indexes = database.IndexDefinitionStorage.IndexDefinitions.ToDictionary(x => x.Key, x => x.Value);
            foreach (var index in indexes)
            {
                var maxIndexOutputsPerDocument = index.Value.MaxIndexOutputsPerDocument;
                var defaultValue = 15;
                if (index.Value.IsMapReduce)
                {
                    defaultValue = 50;
                }
                if (maxIndexOutputsPerDocument != null)
                {
                    if (maxIndexOutputsPerDocument > defaultValue*10 || maxIndexOutputsPerDocument < defaultValue/10)
                    {
                        rangeWarnings.Add($"{index.Value.Name}.MaxIndexOutputsPerDocument",new ExpectedRange
                        {
                            Value = maxIndexOutputsPerDocument.Value,
                            DefaultValue = defaultValue
                        });
                    }
                }

            }
            return rangeWarnings;
        }

        private static Dictionary<string, ExpectedRange> CreateRangeConfigWarnings(DocumentDatabase database)
        {
            Dictionary<string, ExpectedRange> rangeWarnings = new Dictionary<string, ExpectedRange>();
            InMemoryRavenConfiguration imrc = database.Configuration;
            int defaultMaxNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 128 * 1024 : 16 * 1024;
            int defaultInitialNumberOfItemsToIndexInSingleBatch = Environment.Is64BitProcess ? 512 : 256;

            var ravenSettings = new StronglyTypedRavenSettings(imrc.Settings);
            ravenSettings.Setup(defaultMaxNumberOfItemsToIndexInSingleBatch, defaultInitialNumberOfItemsToIndexInSingleBatch);
            Type types = ravenSettings.GetType();
            PropertyInfo[] properties = types.GetProperties();

            foreach (PropertyInfo property in properties)
            {
                var propertyValue = property.GetValue(ravenSettings, null) as IntegerSetting;
                if (propertyValue != null)
                {
                    CheckForRangeWarnings(rangeWarnings, propertyValue, property.Name);
                }
                else
                {
                    var propertyValueObj = property.GetValue(ravenSettings, null);
                    Type objTypes = propertyValueObj.GetType();
                    PropertyInfo[] objProperties = objTypes.GetProperties();

                    foreach (PropertyInfo objProperty in objProperties)
                    {
                        var subpropertyValueObj = objProperty.GetValue(propertyValueObj, null) as IntegerSetting;
                        if (subpropertyValueObj != null)
                        {
                            long result = (long)subpropertyValueObj.DefaultValue * 10;

                            if ((result <= int.MaxValue && subpropertyValueObj.Value > (int)result) || subpropertyValueObj.Value < subpropertyValueObj.DefaultValue / 10)
                            {
                                CheckForRangeWarnings(rangeWarnings, subpropertyValueObj, $"{property.Name}.{objProperty.Name}");
                            }
                        }
                    }
                }
            }
            return rangeWarnings;
        }

        private static void CheckForRangeWarnings(Dictionary<string, ExpectedRange> rangeWarnings, IntegerSetting propertyValue,string name)
        {
            long result = (long)propertyValue.DefaultValue * 10;

            if ((result <= int.MaxValue && propertyValue.Value > (int)result) || propertyValue.Value < propertyValue.DefaultValue / 10)
            {
                rangeWarnings.Add(name, new ExpectedRange
                {
                    Value = propertyValue.Value,
                    DefaultValue = propertyValue.DefaultValue
                });
            }
        }

        public class ExpectedRange
        {
            public int Value { get; set; }
            public int DefaultValue { get; set; }
        }

internal static object GetRequestTrackingForDebug(RequestManager requestManager, string databaseName)
        {
            return requestManager.GetRecentRequests(databaseName).Select(x =>
            {
                var dic = new Dictionary<String, String>();
                foreach (var httpHeader in x.Headers.Value)
                {
                    dic[httpHeader.Key] = httpHeader.Value.First();
                }
                dic.Remove("Authorization");
                dic.Remove("Proxy-Authorization");
                dic.Remove("WWW-Authenticate");
                dic.Remove("Proxy-Authenticate");
                
                return new
                {
                    Uri = x.RequestUri,
                    Method = x.HttpMethod,
                    StatusCode = x.ResponseStatusCode,
                    RequestHeaders = dic.Select(z=>new{Name = z.Key, Values= new[]{z.Value}}),
                    ExecutionTime = string.Format("{0} ms", x.Stopwatch.ElapsedMilliseconds),
                    AdditionalInfo = x.CustomInfo ?? string.Empty
                };
            });
        }

        internal static RavenJObject GetConfigForDebug(DocumentDatabase database)
        {
            var cfg = RavenJObject.FromObject(database.Configuration);
            cfg["OAuthTokenKey"] = "<not shown>";
            var changesAllowed = database.Configuration.Settings[Constants.Versioning.ChangesToRevisionsAllowed];

            if (string.IsNullOrWhiteSpace(changesAllowed) == false)
                cfg[Constants.Versioning.ChangesToRevisionsAllowed] = changesAllowed;

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
            var indexingWork = database .IndexingExecuter.GetCurrentlyProcessingIndexes();
            var reduceWork = database.ReducingExecuter.GetCurrentlyProcessingIndexes();

            var uniqueIndexesBeingProcessed = indexingWork.Union(reduceWork).Distinct(new Index.IndexByIdEqualityComparer()).ToList();
            return new
            {
                NumberOfCurrentlyWorkingIndexes = uniqueIndexesBeingProcessed.Count,
                Indexes = uniqueIndexesBeingProcessed.Select(x =>				
                new
                {
                    IndexName = x.PublicName,
                    IsMapReduce = x.IsMapReduce,
                    CurrentOperations = x.GetCurrentIndexingPerformance().Select(p => new {p.Operation, NumberOfProcessingItems = p.InputCount}),
                    Priority = x.Priority,
                    OverallIndexingRate = x.GetIndexingPerformance().Where(ip => ip.Duration != TimeSpan.Zero).GroupBy(y => y.Operation).Select(g => new
                    {
                        Operation = g.Key,
                        Rate = string.Format("{0:0.0000} ms/doc", g.Sum(z => z.Duration.TotalMilliseconds)/g.Sum(z => z.InputCount))
                    })
                }
                )
            };
        }

        internal static object GetPrefetchingQueueStatusForDebug(DocumentDatabase database)
        {
            var result = new List<object>();
            var totalLoadedDocuments = 0;
            var totalCanceledFutureBatches = 0;
            var totalFaultedFutureBatches = 0;
            List<string> allindexes = new List<string>();
            foreach (var prefetchingBehavior in database.IndexingExecuter.PrefetchingBehaviors)
            {
                var prefetcherDocs = prefetchingBehavior.DebugGetDocumentsInPrefetchingQueue().ToArray();
                var futureBatches = prefetchingBehavior.DebugGetDocumentsInFutureBatches();
                var ioSummary = prefetchingBehavior.DebugIOSummary();

                var compareToCollection = new Dictionary<Etag, int>();

                for (int i = 1; i < prefetcherDocs.Length; i++)
                    compareToCollection.Add(prefetcherDocs[i - 1].Etag, prefetcherDocs[i].Etag.CompareTo(prefetcherDocs[i - 1].Etag));

                var totalLoadedInBatch = prefetcherDocs.Count() + futureBatches.Total;
                totalLoadedDocuments += totalLoadedInBatch;
                totalCanceledFutureBatches += futureBatches.Canceled;
                totalFaultedFutureBatches += futureBatches.Faulted;

                DateTime? lastTimeUsed = prefetchingBehavior.LastTimeUsed != DateTime.MinValue ?
                    prefetchingBehavior.LastTimeUsed :
                    (DateTime?) null;
                TimeSpan? age = lastTimeUsed != null ?
                    TimeSpan.FromTicks((DateTime.UtcNow - prefetchingBehavior.LastTimeUsed).Ticks) :
                    (TimeSpan?) null;

                string indexesText = null;
                if (prefetchingBehavior.Indexes != null)
                {
                    var indexes = prefetchingBehavior.Indexes.Select(y => y.Index.PublicName).ToList();
                    indexesText = string.Join(", ", indexes);
                    allindexes.AddRange(indexes);
                }

                if (compareToCollection.Any(x => x.Value < 0))
                {
                    result.Add(new
                    {
                        Default = prefetchingBehavior.IsDefault,
                        IOSummary = ioSummary,
                        Indexes = indexesText,
                        LastIndexedEtag = prefetchingBehavior.LastIndexedEtag,
                        LastTimeUsed = lastTimeUsed,
                        Age = age,
                        TotalDocs = totalLoadedInBatch,
                        PrefetchedDocsCount = prefetcherDocs.Count(),
                        FutureBatchesDocsCount = futureBatches.Total,
                        HasCorrectlyOrderedEtags = false,
                        IncorrectlyOrderedEtags = compareToCollection.Where(x => x.Value < 0),
                        EtagsWithKeys = prefetcherDocs.ToDictionary(x => x.Etag, x => x.Key),
                        FutureBatchesCount = futureBatches.Summary.Count,
                        CanceledFutureBatches = futureBatches.Canceled,
                        FaultedFutureBatches = futureBatches.Faulted,
                        FutureBatches = futureBatches.Summary
                    });
                }
                else
                {
                    var prefetcherDocsToTake = Math.Min(5, prefetcherDocs.Count());
                    var etagsWithKeysTail = Enumerable.Range(0, prefetcherDocsToTake).Select(
                        i => prefetcherDocs[prefetcherDocs.Count() - prefetcherDocsToTake + i]).ToDictionary(x => x.Etag, x => x.Key);

                    result.Add(new
                    {
                        Default = prefetchingBehavior.IsDefault,
                        IOSummary = ioSummary,
                        Indexes = indexesText,
                        LastIndexedEtag = prefetchingBehavior.LastIndexedEtag,
                        LastTimeUsed = lastTimeUsed,
                        Age = age,
                        TotalDocs = totalLoadedInBatch,
                        PrefetchedDocsCount = prefetcherDocs.Count(),
                        FutureBatchesDocsCount = futureBatches.Total,
                        HasCorrectlyOrderedEtags = true,
                        EtagsWithKeysHead = prefetcherDocs.Take(5).ToDictionary(x => x.Etag, x => x.Key),
                        EtagsWithKeysTail = etagsWithKeysTail,
                        FutureBatchesCount = futureBatches.Summary.Count,
                        CanceledFutureBatches = futureBatches.Canceled,
                        FaultedFutureBatches = futureBatches.Faulted,
                        FutureBatches = futureBatches.Summary
                    });
                }
            }

            var summary = new
            {
                TotalDocs = totalLoadedDocuments,
                TotalPrefetchingBehaviours = database.IndexingExecuter.PrefetchingBehaviors.Count,
                AllIndexes = allindexes.Count > 0 ? string.Join(", ", allindexes) : null,
                CanceledFutureBatches = totalCanceledFutureBatches,
                FaultedFutureBatches = totalFaultedFutureBatches,
                Prefetchers = result.OrderBy(x => ((dynamic)x).Age)
            };

            return summary;
        }
    }
}
