using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.Json;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink.Stats.Performance;

public class LiveCdcSinkPerformanceCollector : DatabaseAwareLivePerformanceCollector<CdcSinkTaskPerformanceStats>
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, CdcSinkProcessAndPerformanceStatsList>> _perCdcSinkProcessStats = new();

    public LiveCdcSinkPerformanceCollector(DocumentDatabase database, IDictionary<string, List<CdcSinkProcess>> cdcSinks) : base(database)
    {
        foreach (var sink in cdcSinks)
        {
            var processes = _perCdcSinkProcessStats.GetOrAdd(sink.Key, s => new ConcurrentDictionary<string, CdcSinkProcessAndPerformanceStatsList>());

            foreach (var sinkProcess in sink.Value)
            {
                processes.TryAdd(sinkProcess.Name, new CdcSinkProcessAndPerformanceStatsList(sinkProcess));
            }
        }

        Start();
    }

    protected override async Task StartCollectingStats()
    {
        Database.CdcSinkLoader.BatchCompleted += BatchCompleted;
        Database.CdcSinkLoader.ProcessAdded += ProcessAdded;
        Database.CdcSinkLoader.ProcessRemoved += ProcessRemoved;

        try
        {
            var stats = Client.Extensions.EnumerableExtension.ForceEnumerateInThreadSafeManner(_perCdcSinkProcessStats)
                .Select(x =>
                {
                    var result = new CdcSinkTaskPerformanceStats
                    {
                        TaskName = x.Key
                    };

                    var perfStats = new List<CdcSinkProcessPerformanceStats>();

                    foreach (var eltAndStats in x.Value)
                    {
                        var process = eltAndStats.Value.Handler;

                        perfStats.Add(new CdcSinkProcessPerformanceStats
                        {

                            Performance = process.GetPerformanceStats()
                        });

                        result.TaskId = process.TaskId;
                    }

                    result.Stats = perfStats.ToArray();

                    return result;
                })
                .ToList();

            Stats.Enqueue(stats);

            await RunInLoop();
        }
        finally
        {
            Database.CdcSinkLoader.BatchCompleted -= BatchCompleted;
            Database.CdcSinkLoader.ProcessAdded -= ProcessAdded;
            Database.CdcSinkLoader.ProcessRemoved -= ProcessRemoved;
        }
    }

    protected override List<CdcSinkTaskPerformanceStats> PreparePerformanceStats()
    {
        var preparedStats = new List<CdcSinkTaskPerformanceStats>(_perCdcSinkProcessStats.Count);

        foreach (var taskProcesses in _perCdcSinkProcessStats)
        {
            List<CdcSinkProcessPerformanceStats> processesStats = null;

            long taskId = -1;

            foreach (var cdcSinkItem in taskProcesses.Value)
            {
                var cdcSinkAndPerformanceStatsList = cdcSinkItem.Value;
                var cdcSink = cdcSinkAndPerformanceStatsList.Handler;
                var performance = cdcSinkAndPerformanceStatsList.Performance;

                var itemsToSend = new List<CdcSinkStatsAggregator>(performance.Count);

                while (performance.TryTake(out CdcSinkStatsAggregator stats))
                {
                    itemsToSend.Add(stats);
                }

                var latestStats = cdcSink.GetLatestPerformanceStats();
                if (latestStats != null &&
                    latestStats.Completed == false &&
                    itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                {
                    if (processesStats == null)
                        processesStats = new List<CdcSinkProcessPerformanceStats>();

                    processesStats.Add(new CdcSinkProcessPerformanceStats
                    {
                        Performance = itemsToSend.Select(item => item.ToPerformanceLiveStatsWithDetails()).ToArray()
                    });

                    taskId = cdcSink.TaskId;
                }
            }

            if (processesStats != null && processesStats.Count > 0)
            {
                preparedStats.Add(new CdcSinkTaskPerformanceStats
                {
                    TaskName = taskProcesses.Key,
                    TaskId = taskId,
                    Stats = processesStats.ToArray()
                });
            }
        }
        return preparedStats;
    }

    protected override void WriteStats(List<CdcSinkTaskPerformanceStats> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
    {
        writer.WriteCdcSinkTaskPerformanceStats(context, stats);
    }

    private void ProcessRemoved(CdcSinkProcess cdcSink)
    {
        if (_perCdcSinkProcessStats.TryGetValue(cdcSink.Configuration.Name, out var processes) == false)
            return;

        processes.TryRemove(cdcSink.Name, out _);
    }

    private void ProcessAdded(CdcSinkProcess cdcSink)
    {
        if (_perCdcSinkProcessStats.TryGetValue(cdcSink.Configuration.Name, out var processes) == false)
            return;

        processes.TryAdd(cdcSink.Name, new CdcSinkProcessAndPerformanceStatsList(cdcSink));
    }

    private void BatchCompleted((string ConfigurationName, string TableName, CdcSinkProcessStatistics Statistics) change)
    {
        if (_perCdcSinkProcessStats.TryGetValue(change.ConfigurationName, out var taskProcesses) == false)
        {
            _perCdcSinkProcessStats.TryAdd(change.ConfigurationName, taskProcesses = new ConcurrentDictionary<string, CdcSinkProcessAndPerformanceStatsList>());
        }

        if (taskProcesses.TryGetValue(change.TableName, out var processAndPerformanceStats) == false)
        {
            var processes = Database.CdcSinkLoader.Processes;

            CdcSinkProcess cdcSink = null;
            foreach (var p in processes)
            {
                if (p.Configuration.Name.Equals(change.ConfigurationName, StringComparison.OrdinalIgnoreCase) &&
                    p.Name.Equals(change.TableName, StringComparison.OrdinalIgnoreCase))
                {
                    cdcSink = p;
                    break;
                }
            }

            if (cdcSink == null)
                return;

            processAndPerformanceStats = new CdcSinkProcessAndPerformanceStatsList(cdcSink);

            taskProcesses.TryAdd(change.TableName, processAndPerformanceStats);
        }

        var latestStat = processAndPerformanceStats.Handler.GetLatestPerformanceStats();
        if (latestStat != null)
            processAndPerformanceStats.Performance.Add(latestStat);
    }

    private class CdcSinkProcessAndPerformanceStatsList : HandlerAndPerformanceStatsList<CdcSinkProcess, CdcSinkStatsAggregator>
    {
        public CdcSinkProcessAndPerformanceStatsList(CdcSinkProcess cdcSink) : base(cdcSink)
        {
            TaskId = cdcSink.TaskId;
        }

        public long TaskId { get; }
    }
}
