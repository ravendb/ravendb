using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime.Internal.Util;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions.Documents.Indexes;
using Raven.Client.ServerWide.Operations;
using Raven.Server.Utils;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;

namespace FastTests;

public partial class RavenTestBase
{
    public readonly IndexesTestBase Indexes;

    public class IndexesTestBase
    {
        private readonly RavenTestBase _parent;

        public IndexesTestBase(RavenTestBase parent)
        {
            _parent = parent ?? throw new ArgumentNullException(nameof(parent));
        }

        public async Task WaitForIndexingInTheClusterAsync(IDocumentStore store, string dbName = null, TimeSpan? timeout = null, bool allowErrors = false)
        {
            var database = dbName ?? store.Database;
            var record = store.Maintenance.Server.Send(new GetDatabaseRecordOperation(database));
            if (record.IsSharded == false)
            {
                foreach (var nodeTag in record.Topology.AllNodes)
                {
                    await WaitForIndexingAsync(store, database, timeout, allowErrors, nodeTag);
                }
                return;
            }

            for (var index = 0; index < record.Sharding.Shards.Count; index++)
            {
                var shard = record.Sharding.Shards[index];
                var shardName = ShardHelper.ToShardName(database, index);
                foreach (string shardNode in shard.Members)
                {
                    await WaitForIndexingAsync(store, shardName, timeout, allowErrors, shardNode);
                }
            }
        }

        public void WaitForIndexing(IDocumentStore store, string databaseName = null, TimeSpan? timeout = null, bool allowErrors = false, string nodeTag = null)
        {
            AsyncHelpers.RunSync(() => WaitForIndexingAsync(store, databaseName, timeout, allowErrors, nodeTag));
        }

        public async Task WaitForIndexingAsync(IDocumentStore store, string databaseName = null, TimeSpan? timeout = null, bool allowErrors = false, string nodeTag = null)
        {
            databaseName ??= store.Database;
            var admin = store.Maintenance.ForDatabase(databaseName);
            var databaseRecord = await admin.Server.SendAsync(new GetDatabaseRecordOperation(databaseName));

            timeout ??= (Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            var nonStaleShards = new HashSet<int>();

            while (sp.Elapsed < timeout.Value)
            {
                var staleStatus = IndexStaleStatus.NonStale;

                if (databaseRecord.IsSharded)
                {
                    foreach (var shardToTopology in databaseRecord.Sharding.Shards)
                    {
                        if (nonStaleShards.Contains(shardToTopology.Key))
                            continue;

                        var shardStatus = await StaleStatusAsync(shardId: shardToTopology.Key);
                        if (shardStatus == IndexStaleStatus.NonStale)
                            nonStaleShards.Add(shardToTopology.Key);

                        staleStatus |= shardStatus;
                    }
                }
                else
                {
                    staleStatus = await StaleStatusAsync();
                }

                if (staleStatus.HasFlag(IndexStaleStatus.Error))
                    break;

                if (staleStatus == IndexStaleStatus.NonStale)
                    return;
            }

            if (allowErrors)
            {
                return;
            }

            var files = new List<string>();
            if (databaseRecord.IsSharded)
            {
                foreach (var shardNumber in databaseRecord.Sharding.Shards.Keys)
                {
                    files.Add(await OutputIndexInfo(shardNumber));
                }
            }
            else
            {
                files.Add(await OutputIndexInfo(null));
            }

            async Task<string> OutputIndexInfo(int? shard)
            {
                IndexPerformanceStats[] perf;
                IndexErrors[] errors;
                IndexStats[] stats;
                if (shard.HasValue == false)
                {
                    perf = await admin.SendAsync(new GetIndexPerformanceStatisticsOperation());
                    errors = await admin.SendAsync(new GetIndexErrorsOperation());
                    stats = await admin.SendAsync(new GetIndexesStatisticsOperation());
                }
                else
                {
                    perf = await admin.ForShard(shard.Value).SendAsync(new GetIndexPerformanceStatisticsOperation());
                    errors = await admin.ForShard(shard.Value).SendAsync(new GetIndexErrorsOperation());
                    stats = await admin.ForShard(shard.Value).SendAsync(new GetIndexesStatisticsOperation());
                }

                var total = new
                {
                    Errors = errors,
                    Stats = stats,
                    Performance = perf,
                    NodeTag = nodeTag
                };

                var file = $"{Path.GetTempFileName()}{(shard != null ? $"_shard{shard}" : "")}.json";

                using (var stream = File.Open(file, FileMode.OpenOrCreate))
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (var writer = new BlittableJsonTextWriter(context, stream))
                {
                    var djv = (DynamicJsonValue)TypeConverter.ToBlittableSupportedType(total);
                    var json = context.ReadObject(djv, "errors");
                    writer.WriteObject(json);
                    writer.Flush();
                }

                return file;
            }

            List<IndexInformation> allIndexes = new();

            if (databaseRecord.IsSharded)
            {
                foreach (var shardNumber in databaseRecord.Sharding.Shards.Keys)
                {
                    var statistics = await admin.ForShard(shardNumber).SendAsync(new GetStatisticsOperation("wait-for-indexing", nodeTag));
                    allIndexes.AddRange(statistics.Indexes);
                }
            }
            else
            {
                var result = await admin.SendAsync(new GetStatisticsOperation("wait-for-indexing", nodeTag));
                allIndexes.AddRange(result.Indexes);
            }

            var corrupted = allIndexes.Where(x => x.State == IndexState.Error).ToList();
            if (corrupted.Count > 0)
            {
                throw new InvalidOperationException(
                    $"The following indexes are with error state: {string.Join(",", corrupted.Select(x => x.Name))} - details at " + string.Join(", ", files));
            }

            throw new TimeoutException("The indexes stayed stale for more than " + timeout.Value + ", stats at " + string.Join(", ", files));

            async Task<IndexStaleStatus> StaleStatusAsync(int? shardId = null)
            {
                var executor = shardId.HasValue ? admin.ForShard(shardId.Value) : admin;
                var databaseStatistics = await executor.SendAsync(new GetStatisticsOperation("wait-for-indexing", nodeTag));
                var indexes = databaseStatistics.Indexes
                    .Where(x => x.State != IndexState.Disabled);

                var staleIndexesCount = indexes.Count(x => x.IsStale || x.Name.StartsWith("ReplacementOf/"));
                if (staleIndexesCount == 0)
                    return IndexStaleStatus.NonStale;

                var erroredIndexesCount = databaseStatistics.Indexes.Count(x => x.State == IndexState.Error);
                if (allowErrors)
                {
                    // wait for all indexes to become non stale
                }
                else if (erroredIndexesCount > 0)
                {
                    // have at least some errors
                    return IndexStaleStatus.Error;
                }

                await Task.Delay(32);
                return IndexStaleStatus.Stale;
            }
        }

        public IndexErrors[] WaitForIndexingErrors(IDocumentStore store, string[] indexNames = null, TimeSpan? timeout = null, string nodeTag = null, bool? errorsShouldExists = null)
        {
            var databaseName = store.Database;
            var admin = store.Maintenance.ForDatabase(databaseName);
            var databaseRecord = admin.Server.Send(new GetDatabaseRecordOperation(databaseName));

            if (errorsShouldExists is null)
            {
                timeout ??= Debugger.IsAttached ? TimeSpan.FromMinutes(15) : TimeSpan.FromMinutes(1);
            }
            else
            {
                timeout ??= errorsShouldExists is true
                    ? TimeSpan.FromSeconds(5)
                    : TimeSpan.FromSeconds(1);
            }

            var toWait = new HashSet<string>(indexNames ?? Array.Empty<string>(), StringComparer.OrdinalIgnoreCase);
            var shardsDict = new Dictionary<string, HashSet<string>>();
            var sp = Stopwatch.StartNew();

            var errors = new List<IndexErrors>();
            if (databaseRecord.IsSharded)
            {
                List<string> shardNames = ShardHelper.GetShardNames(databaseName, databaseRecord.Sharding.Shards.Keys.AsEnumerable()).ToList();
                foreach (var name in shardNames)
                {
                    shardsDict.TryAdd(name, toWait.ToHashSet(StringComparer.OrdinalIgnoreCase));
                }
            }

            while (sp.Elapsed < timeout.Value)
            {
                try
                {
                    if (databaseRecord.IsSharded)
                    {
                        var names = shardsDict.Keys;
                        foreach (var name in names)
                        {
                            var shardNumber = ShardHelper.GetShardNumberFromDatabaseName(name);
                            var indexes = store.Maintenance.ForShard(shardNumber).Send(new GetIndexErrorsOperation(indexNames));

                            foreach (var index in indexes)
                            {
                                if (index.Errors.Length <= 0)
                                    continue;

                                if (shardsDict.TryGetValue(name, out var indexesToWait) == false)
                                    continue;

                                indexesToWait.Remove(index.Name);
                                errors.Add(index);

                                if (indexesToWait.Count == 0)
                                    shardsDict.Remove(name);
                            }

                            if (shardsDict.Count == 0)
                                return errors.ToArray();
                        }
                    }
                    else
                    {
                        var indexes = store.Maintenance.Send(new GetIndexErrorsOperation(indexNames, nodeTag));
                        foreach (var index in indexes)
                        {
                            if (index.Errors.Length > 0)
                            {
                                toWait.Remove(index.Name);

                                if (toWait.Count == 0)
                                    return indexes;
                            }
                        }
                    }

                }
                catch (IndexDoesNotExistException)
                {

                }

                Thread.Sleep(32);
            }

            var msg = $"Got no index error for more than {timeout.Value}.";
            if (toWait.Count != 0)
                msg += $" Still waiting for following indexes: {string.Join(",", toWait)}";
            if (shardsDict.Count != 0)
                msg += $" Still waiting for following shards: {string.Join(", ", shardsDict.Keys)}";

            if (errorsShouldExists is null)
                throw new TimeoutException(msg);

            return null;
        }

        public long WaitForEntriesCount(IDocumentStore store, string indexName, int minEntriesCount, string databaseName = null, TimeSpan? timeout = null, bool throwOnTimeout = true)
        {
            timeout ??= (Debugger.IsAttached
                ? TimeSpan.FromMinutes(15)
                : TimeSpan.FromMinutes(1));

            var sp = Stopwatch.StartNew();
            var entriesCount = -1L;

            while (sp.Elapsed < timeout.Value)
            {
                MaintenanceOperationExecutor operations = string.IsNullOrEmpty(databaseName) == false ? store.Maintenance.ForDatabase(databaseName) : store.Maintenance;

                entriesCount = operations.Send(new GetIndexStatisticsOperation(indexName)).EntriesCount;

                if (entriesCount >= minEntriesCount)
                    return entriesCount;

                Thread.Sleep(32);
            }

            if (throwOnTimeout)
                throw new TimeoutException($"It didn't get min entries count {minEntriesCount} for index {indexName}. The index has {entriesCount} entries.");

            return entriesCount;
        }

        public ManualResetEventSlim WaitForIndexBatchCompleted(IDocumentStore store, Func<(string IndexName, bool DidWork), bool> predicate)
        {
            var database = _parent.GetDatabase(store.Database).Result;

            var mre = new ManualResetEventSlim();

            database.IndexStore.IndexBatchCompleted += x =>
            {
                if (predicate(x))
                    mre.Set();
            };

            return mre;
        }

        [Flags]
        public enum IndexStaleStatus
        {
            NonStale = 0x1,
            Stale = 0x2,
            Error = 0x4
        }
    }
}
