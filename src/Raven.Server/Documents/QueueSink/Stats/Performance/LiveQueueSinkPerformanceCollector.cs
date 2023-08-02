using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL.Queue;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.QueueSink.Stats.Performance;

public class LiveQueueSinkPerformanceCollector : DatabaseAwareLivePerformanceCollector<QueueSinkTaskPerformanceStats>
{
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, QueueSinkProcessAndPerformanceStatsList>> _perQueueSinkProcessStats = new();

    public LiveQueueSinkPerformanceCollector(DocumentDatabase database, Dictionary<string, List<QueueSinkProcess>> queueSinks) : base(database)
    {
        foreach (var sink in queueSinks)
        {
            var processes = _perQueueSinkProcessStats.GetOrAdd(sink.Key, s => new ConcurrentDictionary<string, QueueSinkProcessAndPerformanceStatsList>());

            foreach (var sinkProcess in sink.Value)
            {
                processes.TryAdd(sinkProcess.Script.Name, new QueueSinkProcessAndPerformanceStatsList(sinkProcess));
            }
        }

        Start();
    }

    protected override async Task StartCollectingStats()
    {
        Database.QueueSinkLoader.BatchCompleted += BatchCompleted;
        Database.QueueSinkLoader.ProcessAdded += ProcessAdded;
        Database.QueueSinkLoader.ProcessRemoved += EtlProcessRemoved;

        try
        {
            var stats = Client.Extensions.EnumerableExtension.ForceEnumerateInThreadSafeManner(_perQueueSinkProcessStats)
                .Select(x =>
                {
                    var result = new QueueSinkTaskPerformanceStats
                    {
                        TaskName = x.Key
                    };

                    var perfStats = new List<QueueSinkProcessPerformanceStats>();

                    foreach (var eltAndStats in x.Value)
                    {
                        var process = eltAndStats.Value.Handler;

                        perfStats.Add(new QueueSinkProcessPerformanceStats
                        {
                            TransformationName = process.Script.Name,
                            Performance = process.GetPerformanceStats()
                        });

                        result.BrokerType = process.Configuration.BrokerType;
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
            Database.QueueSinkLoader.BatchCompleted -= BatchCompleted;
            Database.QueueSinkLoader.ProcessAdded -= ProcessAdded;
            Database.QueueSinkLoader.ProcessRemoved -= EtlProcessRemoved;
        }
    }

    protected override List<QueueSinkTaskPerformanceStats> PreparePerformanceStats()
    {
        var preparedStats = new List<QueueSinkTaskPerformanceStats>(_perQueueSinkProcessStats.Count);

        foreach (var taskProcesses in _perQueueSinkProcessStats)
        {
            List<QueueSinkProcessPerformanceStats> processesStats = null;

            var type = QueueBrokerType.None;
            long taskId = -1;

            foreach (var ququeSinkItem in taskProcesses.Value)
            {
                var ququeSinkAndPerformanceStatsList = ququeSinkItem.Value;
                var queueSink = ququeSinkAndPerformanceStatsList.Handler;
                var performance = ququeSinkAndPerformanceStatsList.Performance;

                var itemsToSend = new List<QueueSinkStatsAggregator>(performance.Count);

                while (performance.TryTake(out QueueSinkStatsAggregator stats))
                {
                    itemsToSend.Add(stats);
                }

                var latestStats = queueSink.GetLatestPerformanceStats();
                if (latestStats != null &&
                    latestStats.Completed == false &&
                    itemsToSend.Contains(latestStats) == false)
                    itemsToSend.Add(latestStats);

                if (itemsToSend.Count > 0)
                {
                    if (processesStats == null)
                        processesStats = new List<QueueSinkProcessPerformanceStats>();

                    processesStats.Add(new QueueSinkProcessPerformanceStats
                    {
                        TransformationName = queueSink.Script.Name,
                        Performance = itemsToSend.Select(item => item.ToPerformanceLiveStatsWithDetails()).ToArray()
                    });

                    type = queueSink.Configuration.BrokerType;
                    taskId = queueSink.TaskId;
                }
            }

            if (processesStats != null && processesStats.Count > 0)
            {
                preparedStats.Add(new QueueSinkTaskPerformanceStats
                {
                    TaskName = taskProcesses.Key,
                    TaskId = taskId,
                    BrokerType = type,
                    Stats = processesStats.ToArray()
                });
            }
        }
        return preparedStats;
    }

    protected override void WriteStats(List<QueueSinkTaskPerformanceStats> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
    {
       // TODO arek writer.WriteQueueSinkTaskPerformanceStats(context, stats);
    }

    private void EtlProcessRemoved(QueueSinkProcess queueSink)
    {
        if (_perQueueSinkProcessStats.TryGetValue(queueSink.Configuration.Name, out var processes) == false)
            return;

        processes.TryRemove(queueSink.Script.Name, out _);
    }

    private void ProcessAdded(QueueSinkProcess queueSink)
    {
        if (_perQueueSinkProcessStats.TryGetValue(queueSink.Configuration.Name, out var processes) == false)
            return;

        processes.TryAdd(queueSink.Script.Name, new QueueSinkProcessAndPerformanceStatsList(queueSink));
    }

    private void BatchCompleted((string ConfigurationName, string TransformationName, QueueSinkProcessStatistics Statistics) change)
    {
        if (_perQueueSinkProcessStats.TryGetValue(change.ConfigurationName, out var taskProcesses) == false)
        {
            _perQueueSinkProcessStats.TryAdd(change.ConfigurationName, taskProcesses = new ConcurrentDictionary<string, QueueSinkProcessAndPerformanceStatsList>());
        }

        if (taskProcesses.TryGetValue(change.TransformationName, out var processAndPerformanceStats) == false)
        {
            var processes = Database.QueueSinkLoader.Processes;

            var etl = processes.FirstOrDefault(x => x.Configuration.Name.Equals(change.ConfigurationName, StringComparison.OrdinalIgnoreCase) &&
                                                    x.Script.Name.Equals(change.TransformationName, StringComparison.OrdinalIgnoreCase));

            if (etl == null)
                return;

            processAndPerformanceStats = new QueueSinkProcessAndPerformanceStatsList(etl);

            taskProcesses.TryAdd(change.TransformationName, processAndPerformanceStats);
        }

        var latestStat = processAndPerformanceStats.Handler.GetLatestPerformanceStats();
        if (latestStat != null)
            processAndPerformanceStats.Performance.Add(latestStat);
    }

    private class QueueSinkProcessAndPerformanceStatsList : HandlerAndPerformanceStatsList<QueueSinkProcess, QueueSinkStatsAggregator>
    {
        public QueueSinkProcessAndPerformanceStatsList(QueueSinkProcess queueSink) : base(queueSink)
        {
            TaskId = queueSink.TaskId;
        }

        public long TaskId { get; }
    }
}
