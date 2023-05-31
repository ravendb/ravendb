using System;
using System.Collections.Generic;
using System.IO;
using System.IO.Compression;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Server.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents
{
    public static class DebugInfoAnalyzer
    {
        public class ClusterAnalyzerResult : IDynamicJsonValueConvertible
        {
            public ClusterStats ClusterStats;
            public Dictionary<string, NodeAnalyzerResult> NodesStats;

            public DynamicJsonValue ToJson()
            {
                var djv = new DynamicJsonValue() { ["Cluster Stats"] = ClusterStats };
                foreach (var node in NodesStats.Keys)
                {
                    djv[node] = NodesStats[node];
                }

                return djv;
            }
        }

        public class ClusterStats : IDynamicJsonValueConvertible
        {
            public List<string> IndexEntriesDifference;
            public long CommitIndexDifference;
            public Databases Databases;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue()
                {
                    ["Index Entries Difference"] = IndexEntriesDifference,
                    ["Commit Index Difference"] = CommitIndexDifference,
                    ["Databases"] = Databases
                };
            }
        }

        public class NodeAnalyzerResult : IDynamicJsonValueConvertible
        {
            public Databases Databases;
            public HashSet<string> FirstNodeDatabases;
            public HashSet<string> StaleIndexes;
            public DebugPackageAnalyzerInfo<ClusterLog> ClusterLog;
            public DebugPackageAnalyzerInfo<string> NodeVersion;
            public DebugPackageAnalyzerInfo<string> UpTime;
            public DebugPackageAnalyzerInfo<string> GcTime;
            public long DocumentsCount;
            public long TombstonesCount;
            public long IndexesCount;
            public long RevisionsCount;
            public long ThreadsCount;
            public long TasksCount;
            public DebugPackageAnalyzerInfo<long> ClusterTransactionOperationsCount;
            public long TasksResponsibleNode;
            public DebugPackageAnalyzerInfo<long> ActiveConnections;
            public DebugPackageAnalyzerInfo<bool> LowMemory;
            public DebugPackageAnalyzerInfo<List<string>> LongRequests;
            public DebugPackageAnalyzerInfo<List<string>> LongPings;
            public DebugPackageAnalyzerInfo<List<DynamicJsonValue>> BigThreadsStackTraces;
            public List<DynamicJsonValue> SlowWrites;
            public List<DynamicJsonValue> HugeDocuments;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue()
                {
                    ["Databases"] = Databases,
                    ["Node Version"] = NodeVersion.Success ? NodeVersion.Result : "N/A",
                    ["First Node Databases"] = FirstNodeDatabases,
                    ["Cluster Log"] = ClusterLog.Success ? ClusterLog.Result : "N/A",
                    ["GC Time"] = GcTime.Success ? GcTime.Result : "N/A",
                    ["Documents Count"] = DocumentsCount,
                    ["Tombstones Count"] = TombstonesCount,
                    ["Indexes Count"] = IndexesCount,
                    ["Revisions Count"] = RevisionsCount,
                    ["Threads Count"] = ThreadsCount,
                    ["Tasks Count"] = TasksCount,
                    ["Pending Cluster Transaction Operations"] = ClusterTransactionOperationsCount.Success ? ClusterTransactionOperationsCount.Result : "N/A",
                    ["Tasks Responsible Node"] = TasksResponsibleNode,
                    ["Active Connections"] = ActiveConnections.Success ? ActiveConnections.Result : "N/A",
                    ["Low Memory"] = LowMemory.Success ? LowMemory.Result : "N/A",
                    ["Big Threads Stack Traces"] = BigThreadsStackTraces.Success ? BigThreadsStackTraces.Result : "N/A",
                    ["Long Requests"] = LongRequests.Success ? LongRequests.Result : "N/A",
                    ["Long Pings"] = LongPings.Success ? LongPings.Result : "N/A",
                    ["Stale Indexes"] = StaleIndexes,
                    ["Running Time"] = UpTime.Success ? UpTime.Result : "N/A",
                    ["Slow Writes"] = SlowWrites,
                    ["Huge Documents"] = HugeDocuments
                };
            }
        }

        public class NodeStats
        {
            public string Tag;
            public NodeAnalyzerResult NodeAnalyzerResult;
            public HashSet<string> Databases;
            public ClusterLog ClusterLog;
            public Dictionary<(string Database, string Index), long> IndexesEntries;
        }

        public class Databases : IDynamicJsonValueConvertible
        {
            public int DatabasesCount;
            public HashSet<string> DatabasesNames;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue()
                {
                    ["Databases Count"] = DatabasesCount,
                    ["Databases"] = DatabasesNames
                };
            }
        }

        public class ClusterLog : IDynamicJsonValueConvertible
        {
            public long CommitIndexDifference;
            public long LastLogEntryIndex;

            public DynamicJsonValue ToJson()
            {
                return new DynamicJsonValue()
                {
                    ["Committed Cluster Log Index Difference"] = CommitIndexDifference,
                    ["Last Log Entry Index"] = LastLogEntryIndex
                };
            }
        }

        public static async Task<ClusterAnalyzerResult> GetClusterInfo(Stream clusterStream)
        {
            var analyzer = new ClusterAnalyzerResult
            {
                ClusterStats = new ClusterStats(),
                NodesStats = new Dictionary<string, NodeAnalyzerResult>()
            };
            long minCommitIndexDifference = long.MaxValue;
            long maxCommitIndexDifference = long.MinValue;
            var databases = new HashSet<string>();
            var minIndexEntries = new Dictionary<string, long>();
            var maxIndexEntries = new Dictionary<string, long>();

            using (ZipArchive clusterArchive = new ZipArchive(clusterStream))
            {
                foreach (ZipArchiveEntry entryNode in clusterArchive.Entries)
                {
                    try
                    {
                        NodeStats result = await GetNodeInfo(entryNode.Open());
                        string tag = result.Tag;
                        databases.UnionWith(result.Databases);
                        maxCommitIndexDifference = Math.Max(result.ClusterLog.CommitIndexDifference, maxCommitIndexDifference);
                        minCommitIndexDifference = Math.Min(result.ClusterLog.CommitIndexDifference, minCommitIndexDifference);
                        foreach (var kvp in result.IndexesEntries)
                        {
                            string indexName = kvp.Key.Database + "/" + kvp.Key.Index;
                            if (minIndexEntries.TryGetValue(indexName, out long minNumberOfEntries) == false)
                                minIndexEntries[indexName] = kvp.Value;
                            else
                                minIndexEntries[indexName] = Math.Min(kvp.Value, minNumberOfEntries);

                            if (maxIndexEntries.TryGetValue(indexName, out long maxNumberOfEntries) == false)
                                maxIndexEntries[indexName] = kvp.Value;
                            else
                                maxIndexEntries[indexName] = Math.Max(kvp.Value, maxNumberOfEntries);
                        }

                        analyzer.NodesStats[tag] = result.NodeAnalyzerResult;
                    }
                    catch (Exception e)
                    {
                    }
                }
            }

            var indexEntriesDifference = new List<string>();
            const int indexEntriesDifferenceThreshold = 1000;
            const double indexEntriesPercentageDifferenceThreshold = 0.1;
            foreach (var key in maxIndexEntries.Keys)
            {
                var diff = maxIndexEntries[key] - minIndexEntries[key];
                if (diff > indexEntriesDifferenceThreshold || diff > indexEntriesPercentageDifferenceThreshold * maxIndexEntries[key])
                    indexEntriesDifference.Add($"{key} - {diff}");
            }

            analyzer.ClusterStats.IndexEntriesDifference = indexEntriesDifference;
            analyzer.ClusterStats.CommitIndexDifference = maxCommitIndexDifference - minCommitIndexDifference;
            analyzer.ClusterStats.Databases = new Databases() { DatabasesCount = databases.Count, DatabasesNames = databases };

            return analyzer;
        }

        public static async Task<NodeStats> GetNodeInfo(Stream nodeStream)
        {
            long tasksCount = 0;
            var nodeAnalyzerResult = new NodeAnalyzerResult();
            var staleIndexes = new HashSet<string>();
            var indexEntries = new Dictionary<(string Database, string Index), long>();
            var slowWrites = new List<DynamicJsonValue>();
            var hugeDocuments = new List<DynamicJsonValue>();
            var firstNodeDatabases = new HashSet<string>();
            var databases = new HashSet<string>();
            long tasksResponsibleNode = 0;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (ZipArchive archive = new ZipArchive(nodeStream))
            {
                var archiveEntries = new Dictionary<string, ZipArchiveEntry>();
                foreach (ZipArchiveEntry entry in archive.Entries)
                {
                    archiveEntries[entry.FullName] = entry;
                    string databaseName = GetDatabaseName(entry.FullName);
                    if (databaseName != "server-wide")
                        databases.Add(databaseName);
                }

                if (databases.Any() == false)
                    throw new Exception("There are no databases in the node");

                NodeTagInfo nodeTagInfo = new NodeTagInfo(context, archiveEntries, "server-wide/cluster.topology.json");
                await nodeTagInfo.GetInfo();
                if (nodeTagInfo.Success == false)
                    throw nodeTagInfo.Exception;

                var nodeTag = nodeTagInfo.Result;

                UrlToNodeInfo urlToNodeInfo = new UrlToNodeInfo(context, archiveEntries, "server-wide/cluster.topology.json");
                await urlToNodeInfo.GetInfo();
                if (urlToNodeInfo.Success == false)
                    throw urlToNodeInfo.Exception;

                var urlsToNodes = urlToNodeInfo.Result;

                LowMemoryInfo lowMemoryInfo = new LowMemoryInfo(context, archiveEntries, "server-wide/admin.memory.stats.json");
               await lowMemoryInfo.GetInfo();

                ElectionTimeoutInfo electionTimeoutInfo = new ElectionTimeoutInfo(context, archiveEntries, databases.First() + "/admin.configuration.settings.json");
                await electionTimeoutInfo.GetInfo();
                if (electionTimeoutInfo.Success == false)
                    throw electionTimeoutInfo.Exception;

                var electionTimeout = electionTimeoutInfo.Result;

                ThreadsInfo threadsInfo = new ThreadsInfo(context, archiveEntries, "server-wide/admin.memory.stats.json");
                await threadsInfo.GetInfo();
                if (threadsInfo.Success == false)
                    throw threadsInfo.Exception;
                
                // server-wide
                UpTimeInfo upTimeInfo = new UpTimeInfo(context, archiveEntries, "server-wide/admin.stats.json");
                await upTimeInfo.GetInfo();

                LongRequestsInfo longRequestsInfo = new LongRequestsInfo(context, archiveEntries, "server-wide/requestTimes.txt");
                await longRequestsInfo.GetInfo();

                BigThreadsStackTracesInfo bigThreadsStackTracesInfo = new BigThreadsStackTracesInfo(context, archiveEntries, "server-wide/admin.threads.stack-trace.json", threadsInfo.BigThreads);
                await bigThreadsStackTracesInfo.GetInfo();

                LongPingsInfo longPingsInfo = new LongPingsInfo(context, archiveEntries, "server-wide/admin.node.ping.json", electionTimeout, nodeTag, urlsToNodes);
                await longPingsInfo.GetInfo();

                NodeVersionInfo nodeVersionInfo = new NodeVersionInfo(context, archiveEntries, "server-wide/build.version.json");
                await nodeVersionInfo.GetInfo();

                ClusterLogInfo clusterLogInfo = new ClusterLogInfo(context, archiveEntries, "server-wide/admin.cluster.log.json");
                await clusterLogInfo.GetInfo();

                ActiveConnectionsInfo activeConnectionsInfo = new ActiveConnectionsInfo(context, archiveEntries, "server-wide/admin.info.tcp.active-connections.json");
                await activeConnectionsInfo.GetInfo();

                GcTimeInfo gcTimeInfo = new GcTimeInfo(context, archiveEntries, "server-wide/admin.memory.gc.json");
                await gcTimeInfo.GetInfo();

                // databases
                foreach (var database in databases)
                {
                    SlowWritesInfo slowWritesInfo = new SlowWritesInfo(context, archiveEntries, database + "/slow-writes.json", database);
                    await slowWritesInfo.GetInfo();
                    foreach (var slowWrite in slowWritesInfo.Result)
                    {
                        slowWrites.Add(slowWrite);
                    }

                    HugeDocumentsInfo hugeDocumentsInfo = new HugeDocumentsInfo(context, archiveEntries, database + "/documents.huge.json", database);
                    await hugeDocumentsInfo.GetInfo();
                    foreach (var hugeDocument in hugeDocumentsInfo.Result)
                    {
                        hugeDocuments.Add(hugeDocument);
                    }

                    DatabaseStatsInfo databaseStatsInfo = new DatabaseStatsInfo(context, archiveEntries, database + "/stats.json");
                    await databaseStatsInfo.GetInfo();
                    nodeAnalyzerResult.IndexesCount += databaseStatsInfo.IndexesCount;
                    nodeAnalyzerResult.DocumentsCount += databaseStatsInfo.DocumentsCount;
                    nodeAnalyzerResult.TombstonesCount += databaseStatsInfo.TombstonesCount;
                    nodeAnalyzerResult.RevisionsCount += databaseStatsInfo.RevisionsCount;

                    TasksInfo tasksInfo = new TasksInfo(context, archiveEntries, database + "/tasks.json", nodeTag, tasksCount, tasksResponsibleNode);
                    await tasksInfo.GetInfo();
                    tasksCount = tasksInfo.TasksCount;
                    tasksResponsibleNode = tasksInfo.TasksResponsibleNode;

                    StaleIndexesInfo staleIndexesInfo = new StaleIndexesInfo(context, archiveEntries, database + "/indexes.stats.json", database);
                    await staleIndexesInfo.GetInfo();
                    foreach (var staleIndex in staleIndexesInfo.Result)
                    {
                        staleIndexes.Add(staleIndex);
                    }

                    ClusterTransactionOperationsInfo clusterTransactionOperationsInfo = new ClusterTransactionOperationsInfo(context, archiveEntries, database + "/admin.cluster.txinfo.json");
                    await clusterTransactionOperationsInfo.GetInfo();
                    nodeAnalyzerResult.ClusterTransactionOperationsCount = clusterTransactionOperationsInfo;

                    IndexEntriesInfo indexEntriesInfo = new IndexEntriesInfo(context, archiveEntries, database + "/indexes.stats.json", database);
                    await indexEntriesInfo.GetInfo();
                    foreach (var kvp in indexEntriesInfo.Result)
                    {
                        indexEntries[kvp.Key] = kvp.Value;
                    }

                    FirstNodeDatabasesInfo firstNodeDatabasesInfo = new FirstNodeDatabasesInfo(context, archiveEntries, database + "/database-record.json", nodeTag);
                    await firstNodeDatabasesInfo.GetInfo();
                    if (firstNodeDatabasesInfo.Result != null)
                    {
                        firstNodeDatabases.Add(firstNodeDatabasesInfo.Result);
                    }
                }

                nodeAnalyzerResult.Databases = new Databases
                {
                    DatabasesCount = databases.Count,
                    DatabasesNames = databases
                };
                nodeAnalyzerResult.NodeVersion = nodeVersionInfo;
                nodeAnalyzerResult.FirstNodeDatabases = firstNodeDatabases;
                nodeAnalyzerResult.ClusterLog = clusterLogInfo;
                nodeAnalyzerResult.GcTime = gcTimeInfo;
                nodeAnalyzerResult.ThreadsCount = threadsInfo.ThreadsCount;
                nodeAnalyzerResult.TasksCount = tasksCount;
                nodeAnalyzerResult.TasksResponsibleNode = tasksResponsibleNode;
                nodeAnalyzerResult.ActiveConnections = activeConnectionsInfo;
                nodeAnalyzerResult.LowMemory = lowMemoryInfo;
                nodeAnalyzerResult.BigThreadsStackTraces = bigThreadsStackTracesInfo;
                nodeAnalyzerResult.LongRequests = longRequestsInfo;
                nodeAnalyzerResult.LongPings = longPingsInfo;
                nodeAnalyzerResult.StaleIndexes = staleIndexes;
                nodeAnalyzerResult.UpTime = upTimeInfo;
                nodeAnalyzerResult.SlowWrites = slowWrites;
                nodeAnalyzerResult.HugeDocuments = hugeDocuments;

                return new NodeStats()
                {
                    Tag = nodeTag,
                    NodeAnalyzerResult = nodeAnalyzerResult,
                    Databases = databases,
                    ClusterLog = clusterLogInfo.Success ? clusterLogInfo.Result : new ClusterLog(),
                    IndexesEntries = indexEntries
                };
            }
        }

        public abstract class DebugPackageAnalyzerInfo<T>
        {
            public bool Success = false;
            public readonly string FileName;
            public Exception Exception;
            public ZipArchiveEntry Entry;
            public readonly JsonOperationContext Context;
            public readonly Dictionary<string, ZipArchiveEntry> ArchiveEntries;
            public T Result;

            protected DebugPackageAnalyzerInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName)
            {
                Context = context;
                ArchiveEntries = archiveEntries;
                FileName = fileName;
            }

            public async Task GetInfo()
            {
                BlittableJsonReaderObject blittable = null;
                const string requestTimesSuffix = ".txt";

                if (ArchiveEntries.TryGetValue(FileName, out Entry) == false)
                {
                    Exception = new KeyNotFoundException($"Could not find file '{FileName}' in '{GetType()}'");
                    return;
                }
                try
                {
                    if (Entry.Name.EndsWith(requestTimesSuffix) == false)
                        blittable = await Context.ReadForMemoryAsync(Entry.Open(), Entry.FullName);

                    Result = GetInfoInternal(blittable);
                    Success = true;
                }
                catch (Exception e)
                {
                    Exception = new Exception($"Error in '{GetType()}' in file: '{FileName}'", e);
                }
            }

            public abstract T GetInfoInternal(BlittableJsonReaderObject zipArchiveEntry);

            public void ThrowFieldNotFound(string fieldName)
            {
                throw new Exception($"Could not find the '{fieldName}' field in '{FileName}'");
            }
        }

        private class NodeTagInfo : DebugPackageAnalyzerInfo<string>
        {
            public NodeTagInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {
                
            }

            public override string GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("@metadata", out BlittableJsonReaderObject metadata) == false)
                    ThrowFieldNotFound("@metadata");
                
                if (metadata.TryGet(nameof(ServerStore.NodeTag), out string nodeTag) == false)
                    ThrowFieldNotFound(nameof(ServerStore.NodeTag));

                return nodeTag;
            }
        }

        private class FirstNodeDatabasesInfo : DebugPackageAnalyzerInfo<string>
        {
            public string NodeTag;

            public FirstNodeDatabasesInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, string nodeTag) : base(context, archiveEntries, fileName)
            {
                NodeTag = nodeTag;
            }

            public override string GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet(nameof(RawDatabaseRecord.Topology), out BlittableJsonReaderObject topology))
                    ThrowFieldNotFound(nameof(RawDatabaseRecord.Topology));
                    
                if (blittable.TryGet(nameof(RawDatabaseRecord.DatabaseName), out string dbName) == false)
                    ThrowFieldNotFound(nameof(RawDatabaseRecord.DatabaseName));

                if (topology.TryGet(nameof(RawDatabaseRecord.Topology.Members), out BlittableJsonReaderArray members) == false)
                    ThrowFieldNotFound(nameof(RawDatabaseRecord.Topology.Members));

                if (NodeTag == members[0].ToString())
                    return dbName;
                else
                    return null;
            }
        }

        private class ClusterTransactionOperationsInfo : DebugPackageAnalyzerInfo<long>
        {
            public ClusterTransactionOperationsInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override long GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("Count", out long count) == false)
                    ThrowFieldNotFound("Count");
                    
                return count;
            }
        }

        private class LowMemoryInfo : DebugPackageAnalyzerInfo<bool>
        {
            public LowMemoryInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override bool GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("MemoryInformation", out BlittableJsonReaderObject memoryInformation) == false)
                    ThrowFieldNotFound("MemoryInformation");

                if (memoryInformation.TryGet("AvailableMemory", out string availableMemory) == false)
                    ThrowFieldNotFound("AvailableMemory");

                string[] size = availableMemory.Split(' ');
                return size[1] == "MBytes" || size[1] == "KBytes" || size[1] == "Bytes";
            }
        }

        private class GcTimeInfo : DebugPackageAnalyzerInfo<string>
        {
            public GcTimeInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override string GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("Any", out BlittableJsonReaderObject gcInfo) == false)
                    ThrowFieldNotFound("Any");

                if (gcInfo.TryGet("PauseDurations", out BlittableJsonReaderArray time) == false)
                    ThrowFieldNotFound("PauseDurations");

                return time[0].ToString();
            }
        }

        private class UpTimeInfo : DebugPackageAnalyzerInfo<string>
        {
            public UpTimeInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override string GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("UpTime", out string upTime) == false)
                    ThrowFieldNotFound("UpTime");

                return upTime;
            }
        }

        private class LongRequestsInfo : DebugPackageAnalyzerInfo<List<string>>
        {
            public LongRequestsInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override List<string> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var longRequests = new List<string>();
                var stream = new StreamReader(Entry.Open(), Encoding.Default);
                while (stream.EndOfStream != true)
                {
                    string line = stream.ReadLine();
                    string[] split = line.Split(',');
                    string time = split[0];
                    string hoursAndMinutes = time.Substring(0, 5);
                    if (hoursAndMinutes == "00:00" || hoursAndMinutes == "00:01")
                        continue;
                    string request = split[1].Substring(1);
                    longRequests.Add($"{time} - {request}");
                }

                stream.Close();
                return longRequests;
            }
        }

        private class LongPingsInfo : DebugPackageAnalyzerInfo<List<string>>
        {
            public long ElectionTimeout;
            public string NodeTag;
            public Dictionary<string, string> UrlsToNodes;

            public LongPingsInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, long electionTimeout, string nodeTag, Dictionary<string, string> urlsToNodes) : base(context, archiveEntries, fileName)
            {
                ElectionTimeout = electionTimeout;
                NodeTag = nodeTag;
                UrlsToNodes = urlsToNodes;
            }

            public override List<string> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var longPings = new List<string>();
                if (blittable.TryGet("Result", out BlittableJsonReaderArray pingResults) == false)
                    ThrowFieldNotFound("Result");

                const long longPingThreshold = 1000;
                bool electionTimeoutIsLower = ElectionTimeout < longPingThreshold;
                string longPingCause = electionTimeoutIsLower ? $"election timeout - {ElectionTimeout}ms" : $"{longPingThreshold}ms threshold";
                foreach (BlittableJsonReaderObject result in pingResults)
                {
                    if (result.TryGet("TcpInfo", out BlittableJsonReaderObject tcpInfo) == false)
                        ThrowFieldNotFound("TcpInfo");
                    if (tcpInfo.TryGet("ReceiveTime", out long receiveTime) == false)
                        ThrowFieldNotFound("ReceiveTime");
                    if (receiveTime <= Math.Min(ElectionTimeout, longPingThreshold))
                        continue;
                    result.TryGet("Url", out string url);
                    longPings.Add($"From {NodeTag} to {UrlsToNodes[url]}, Ping time {receiveTime}ms was higher than {longPingCause}");
                }

                return longPings;
            }
        }

        private class ActiveConnectionsInfo : DebugPackageAnalyzerInfo<long>
        {
            public ActiveConnectionsInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override long GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("TotalConnections", out long connections) == false)
                    ThrowFieldNotFound("TotalConnections");

                return connections;
            }
        }

        private class ClusterLogInfo : DebugPackageAnalyzerInfo<ClusterLog>
        {
            public ClusterLogInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override ClusterLog GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("CommitIndex", out long commitIndex) == false)
                    ThrowFieldNotFound("CommitIndex");

                if (blittable.TryGet("LastLogEntryIndex", out long lastLogEntryIndex) == false)
                    ThrowFieldNotFound("LastLogEntryIndex");

                return new ClusterLog()
                {
                    CommitIndexDifference = commitIndex - lastLogEntryIndex,
                    LastLogEntryIndex = lastLogEntryIndex
                };
            }
        }

        private class UrlToNodeInfo : DebugPackageAnalyzerInfo<Dictionary<string, string>>
        {
            public UrlToNodeInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override Dictionary<string, string> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var urlsToNodes = new Dictionary<string, string>();
                if (blittable.TryGet("Topology", out BlittableJsonReaderObject allNodes) == false)
                    ThrowFieldNotFound("Topology");

                if (allNodes.TryGet("AllNodes", out BlittableJsonReaderObject nodes) == false)
                    ThrowFieldNotFound("AllNodes");

                foreach (string node in nodes.GetPropertyNames())
                {
                    if (nodes.TryGet(node, out string nodeUrl))
                        urlsToNodes[nodeUrl] = node;
                }

                return urlsToNodes;
            }
        }

        private class NodeVersionInfo : DebugPackageAnalyzerInfo<string>
        {
            public NodeVersionInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override string GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("FullVersion", out string fullVersion) == false)
                    ThrowFieldNotFound("FullVersion");

                return fullVersion;
            }
        }

        private class DatabaseStatsInfo : DebugPackageAnalyzerInfo<object>
        {
            public long IndexesCount;
            public long DocumentsCount;
            public long TombstonesCount;
            public long RevisionsCount;

            public DatabaseStatsInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override object GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet(nameof(DatabaseStatistics.CountOfIndexes), out int numberOfIndexes) == false)
                    ThrowFieldNotFound(nameof(DatabaseStatistics.CountOfIndexes));

                if (blittable.TryGet(nameof(DatabaseStatistics.CountOfDocuments), out int documents) == false)
                    ThrowFieldNotFound(nameof(DatabaseStatistics.CountOfDocuments));

                if (blittable.TryGet(nameof(DatabaseStatistics.CountOfTombstones), out int tombstones) == false)
                    ThrowFieldNotFound(nameof(DatabaseStatistics.CountOfTombstones));

                if (blittable.TryGet(nameof(DatabaseStatistics.CountOfRevisionDocuments), out int revisions) == false)
                    ThrowFieldNotFound(nameof(DatabaseStatistics.CountOfRevisionDocuments));

                IndexesCount = numberOfIndexes;
                DocumentsCount = documents;
                TombstonesCount = tombstones;
                RevisionsCount = revisions;

                return null;
            }
        }

        private class TasksInfo : DebugPackageAnalyzerInfo<object>
        {
            public string NodeTag;
            public long TasksCount;
            public long TasksResponsibleNode;

            public TasksInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, string nodeTag, long tasksCount, long tasksResponsibleNode) : base(context, archiveEntries, fileName)
            {
                NodeTag = nodeTag;
                TasksCount = tasksCount;
                TasksResponsibleNode = tasksResponsibleNode;
            }

            public override object GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("OngoingTasksList", out BlittableJsonReaderArray tasks))
                    ThrowFieldNotFound("OngoingTasksList");

                TasksCount += tasks.Length;
                foreach (BlittableJsonReaderObject task in tasks)
                {
                    if (task.TryGet("ResponsibleNode", out BlittableJsonReaderObject node) == false)
                        ThrowFieldNotFound("ResponsibleNode");
                    if (node.TryGet("NodeTag", out string tag) == false)
                        ThrowFieldNotFound("NodeTag");
                    if (tag == NodeTag)
                        TasksResponsibleNode++;
                }

                return null;
            }
        }

        private class StaleIndexesInfo : DebugPackageAnalyzerInfo<List<string>>
        {
            public string DatabaseName;

            public StaleIndexesInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, string databaseName) : base(context, archiveEntries, fileName)
            {
                DatabaseName = databaseName;
            }

            public override List<string> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var staleIndexes = new List<string>();
                if (blittable.TryGet("Results", out BlittableJsonReaderArray indexes) == false)
                    ThrowFieldNotFound("Results");

                foreach (BlittableJsonReaderObject index in indexes)
                {
                    if (index.TryGet("IsStale", out bool isStale) == false)
                        ThrowFieldNotFound("IsStale");
                    if (index.TryGet("Name", out string name) == false)
                        ThrowFieldNotFound("Name");
                    if (isStale)
                        staleIndexes.Add(DatabaseName + "/" + name);
                }

                return staleIndexes;
            }
        }

        private class IndexEntriesInfo : DebugPackageAnalyzerInfo<Dictionary<(string Database, string Index), long>>
        {
            public string DatabaseName;

            public IndexEntriesInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, string databaseName) : base(context, archiveEntries, fileName)
            {
                DatabaseName = databaseName;
            }

            public override Dictionary<(string Database, string Index), long> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var indexEntries = new Dictionary<(string Database, string Index), long>();
                if (blittable.TryGet("Results", out BlittableJsonReaderArray indexes) == false)
                    ThrowFieldNotFound("Results");
                foreach (BlittableJsonReaderObject index in indexes)
                {
                    if (index.TryGet("Name", out string indexName) == false)
                        ThrowFieldNotFound("Name");
                    if(index.TryGet("EntriesCount", out long entriesCount) == false)
                        ThrowFieldNotFound("EntriesCount");

                    indexEntries.Add((DatabaseName, indexName), entriesCount);
                }

                return indexEntries;
            }
        }

        private class HugeDocumentsInfo : DebugPackageAnalyzerInfo<List<DynamicJsonValue>>
        {
            public string DatabaseName;

            public HugeDocumentsInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, string databaseName) : base(context, archiveEntries, fileName)
            {
                DatabaseName = databaseName;
            }

            public override List<DynamicJsonValue> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var hugeDocuments = new List<DynamicJsonValue>();
                if (blittable.TryGet("Results", out BlittableJsonReaderArray documents) == false)
                    ThrowFieldNotFound("Results");

                foreach (BlittableJsonReaderObject document in documents)
                {
                    if (document.TryGet("Id", out string id) == false)
                        ThrowFieldNotFound("Id");
                    if (document.TryGet("LastAccess", out DateTime lastAccess) == false)
                        ThrowFieldNotFound("LastAccess");
                    if (document.TryGet("Size", out long size) == false)
                        ThrowFieldNotFound("Size");

                    var json = new DynamicJsonValue()
                    {
                        ["Database"] = DatabaseName,
                        ["Id"] = id,
                        ["Size"] = size,
                        ["LastAccess"] = lastAccess
                    };

                    hugeDocuments.Add(json);
                }

                return hugeDocuments;
            }
        }

        private class SlowWritesInfo : DebugPackageAnalyzerInfo<List<DynamicJsonValue>>
        {
            public string DatabaseName;

            public SlowWritesInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, string databaseName) : base(context, archiveEntries, fileName)
            {
                DatabaseName = databaseName;
            }

            public override List<DynamicJsonValue> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var slowWrites = new List<DynamicJsonValue>();
                if (blittable.TryGet("Writes", out BlittableJsonReaderObject writes) == false)
                    ThrowFieldNotFound("Writes");

                foreach (var property in writes.GetPropertyNames())
                {
                    if (writes.TryGet(property, out BlittableJsonReaderObject writeInfo) == false)
                        ThrowFieldNotFound(property);

                    var write = new DynamicJsonValue();
                    write[property] = WriteToDjv(writeInfo);

                    DynamicJsonValue WriteToDjv(BlittableJsonReaderObject blittableJsonReaderObject)
                    {
                        var json = new DynamicJsonValue();
                        json["Database"] = DatabaseName;
                        foreach (var prop in blittableJsonReaderObject.GetPropertyNames())
                        {
                            if (blittableJsonReaderObject.TryGet(prop, out object propVal))
                                json[prop] = propVal.ToString();
                        }

                        return json;
                    }

                    slowWrites.Add(write);
                }

                return slowWrites;
            }
        }

        private class ElectionTimeoutInfo : DebugPackageAnalyzerInfo<long>
        {
            public ElectionTimeoutInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override long GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("Settings", out BlittableJsonReaderArray settings) == false)
                    ThrowFieldNotFound("Settings");
                foreach (BlittableJsonReaderObject setting in settings)
                {
                    if (setting.TryGet("Metadata", out BlittableJsonReaderObject metadata) == false)
                        ThrowFieldNotFound("Metadata");
                    if (metadata.TryGet("Keys", out BlittableJsonReaderArray keys) == false)
                        ThrowFieldNotFound("Keys");
                    foreach (var key in keys)
                    {
                        if (key.ToString() != "Cluster.ElectionTimeoutInMs")
                            continue;
                        if (setting.TryGet("ServerValues", out BlittableJsonReaderObject serverValues) == false)
                            ThrowFieldNotFound("ServerValues");
                        if (serverValues.TryGet("Cluster.ElectionTimeoutInMs", out BlittableJsonReaderObject electionTimeoutInMs))
                        {
                            if (electionTimeoutInMs.TryGet("Value", out long value))
                                return value;
                        }
                        if (metadata.TryGet("DefaultValue", out long defaultValue) == false)
                            ThrowFieldNotFound("DefaultValue");

                        return defaultValue;
                    }
                }

                throw new Exception("Could not find election timeout length");
            }
        }

        private class ThreadsInfo : DebugPackageAnalyzerInfo<object>
        {
            public long ThreadsCount;
            public Dictionary<long, (string ThreadName, string Size)> BigThreads;

            public ThreadsInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName) : base(context, archiveEntries, fileName)
            {

            }

            public override object GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                if (blittable.TryGet("Threads", out BlittableJsonReaderArray threads) == false)
                    ThrowFieldNotFound("Threads");

                ThreadsCount = threads.Length;
                BigThreads = GetBigThreads(threads);

                return null;
            }

            private Dictionary<long, (string ThreadName, string Size)> GetBigThreads(BlittableJsonReaderArray threads)
            {
                var bigThreads = new Dictionary<long, (string ThreadName, string Size)>();
                foreach (BlittableJsonReaderObject thread in threads)
                {
                    if (thread.TryGet("HumaneAllocations", out string memory) == false)
                        ThrowFieldNotFound("HumaneAllocations");
                    if(thread.TryGet("Name", out string name) == false)
                        ThrowFieldNotFound("Name");
                    string[] size = memory.Split(' ');
                    if (size[1] != "GBytes" && size[1] != "TBytes")
                        continue;
                    if (thread.TryGet("Id", out long id))
                        bigThreads[id] = (name, memory);

                    else if (thread.TryGet("Ids", out BlittableJsonReaderArray ids))
                    {
                        foreach (BlittableJsonReaderObject subThread in ids)
                        {
                            if (subThread.TryGet("HumaneAllocations", out string subThreadMemory) == false)
                                ThrowFieldNotFound("HumaneAllocations");
                            string[] subThreadSize = subThreadMemory.Split(' ');
                            if (subThreadSize[1] != "GBytes" && subThreadSize[1] != "TBytes")
                                continue;
                            if (subThread.TryGet("Id", out long subThreadId))
                                ThrowFieldNotFound("Id");

                            bigThreads[subThreadId] = (name, subThreadMemory);
                        }
                    }
                }

                return bigThreads;
            }
        }

        private class BigThreadsStackTracesInfo : DebugPackageAnalyzerInfo<List<DynamicJsonValue>>
        {
            public Dictionary<long, (string ThreadName, string Size)> BigThreads;

            public BigThreadsStackTracesInfo(JsonOperationContext context, Dictionary<string, ZipArchiveEntry> archiveEntries, string fileName, Dictionary<long, (string ThreadName, string Size)> bigThreads) : base(context, archiveEntries, fileName)
            {
                BigThreads = bigThreads;
            }

            public override List<DynamicJsonValue> GetInfoInternal(BlittableJsonReaderObject blittable)
            {
                var bigThreadsStackTraces = new List<DynamicJsonValue>();
                var djv = new DynamicJsonValue();
                if (blittable.TryGet("Results", out BlittableJsonReaderArray results) == false)
                    ThrowFieldNotFound("Results");

                foreach (BlittableJsonReaderObject result in results)
                {
                    if (result.TryGet("ThreadIds", out BlittableJsonReaderArray ids) == false)
                        ThrowFieldNotFound("ThreadIds");
                    if (result.TryGet("StackTrace", out BlittableJsonReaderArray stackTrace) == false)
                        ThrowFieldNotFound("StackTrace");
                    foreach (long id in ids)
                    {
                        if (BigThreads.TryGetValue(id, out (string ThreadName, string Size) tuple) == false)
                            continue;
                        djv["Name"] = tuple.ThreadName;
                        djv["Id"] = id;
                        djv["Size"] = tuple.Size;
                        djv["Stack Trace"] = stackTrace.ToString();

                        bigThreadsStackTraces.Add(djv);
                    }
                }

                return bigThreadsStackTraces;
            }
        }

        private static string GetDatabaseName(string path)
        {
            return path.Split('/')[0];
        }
    }
}
