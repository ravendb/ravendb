using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL.Stats;
using Raven.Server.Json;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.ETL
{
    public class LiveEtlPerformanceCollector : LivePerformanceCollector<EtlTaskPerformanceStats>
    {
        private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, EtlProcessAndPerformanceStatsList>> _perEtlProcessStats =
            new ConcurrentDictionary<string, ConcurrentDictionary<string, EtlProcessAndPerformanceStatsList>>();

        public LiveEtlPerformanceCollector(DocumentDatabase database, Dictionary<string, List<EtlProcess>> etls) : base(database)
        {
            foreach (var etl in etls)
            {
                var processes = _perEtlProcessStats.GetOrAdd(etl.Key, s => new ConcurrentDictionary<string, EtlProcessAndPerformanceStatsList>());

                foreach (var etlProcess in etl.Value)
                {
                    processes.TryAdd(etlProcess.TransformationName, new EtlProcessAndPerformanceStatsList(etlProcess));
                }
            }

            Start();
        }

        protected override async Task StartCollectingStats()
        {
            Database.EtlLoader.BatchCompleted  += BatchCompleted;

            try
            {
                var stats = Client.Extensions.EnumerableExtension.ForceEnumerateInThreadSafeManner(_perEtlProcessStats)
                    .Select(x =>
                    {
                        var result = new EtlTaskPerformanceStats()
                        {
                            TaskName = x.Key,
                        };

                        var perfStats = new List<EtlProcessPerformanceStats>();

                        foreach (var eltAndStats in x.Value)
                        {
                            var process = eltAndStats.Value.Handler;

                            perfStats.Add(new EtlProcessPerformanceStats
                            {
                                TransformationName = process.TransformationName,
                                Performance = process.GetPerformanceStats()
                            });

                            result.EtlType = process.EtlType;
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
                Database.EtlLoader.BatchCompleted -= BatchCompleted;
            }
        }

        private void BatchCompleted((string ConfigurationName, string TransformationName, EtlProcessStatistics Statistics) change)
        {
            if (_perEtlProcessStats.TryGetValue(change.ConfigurationName, out var taskProcesses) == false)
            {
                _perEtlProcessStats.TryAdd(change.ConfigurationName, taskProcesses = new ConcurrentDictionary<string, EtlProcessAndPerformanceStatsList>());
            }

            if (taskProcesses.TryGetValue(change.TransformationName, out var processAndPerformanceStats) == false)
            {
                var processes = Database.EtlLoader.Processes;

                var etl = processes.FirstOrDefault(x => x.ConfigurationName.Equals(change.ConfigurationName, StringComparison.OrdinalIgnoreCase) &&
                                                    x.TransformationName.Equals(change.TransformationName, StringComparison.OrdinalIgnoreCase));

                if (etl == null)
                    return;
                
                processAndPerformanceStats = new EtlProcessAndPerformanceStatsList(etl);

                taskProcesses.TryAdd(change.TransformationName, processAndPerformanceStats);
            }

            var latestStat = processAndPerformanceStats.Handler.GetLatestPerformanceStats();
            if (latestStat != null)
                processAndPerformanceStats.Performance.Add(latestStat, CancellationToken);
        }

        protected override List<EtlTaskPerformanceStats> PreparePerformanceStats()
        {
            var preparedStats = new List<EtlTaskPerformanceStats>(_perEtlProcessStats.Count);

            foreach (var taskProcesses in _perEtlProcessStats)
            {
                List<EtlProcessPerformanceStats> processesStats = null;

                var type = EtlType.Raven;

                foreach (var etlItem in taskProcesses.Value)
                {
                    var etlAndPerformanceStatsList = etlItem.Value;
                    var etl = etlAndPerformanceStatsList.Handler;
                    var performance = etlAndPerformanceStatsList.Performance;

                    var itemsToSend = new List<EtlStatsAggregator>(performance.Count);

                    while (performance.TryTake(out EtlStatsAggregator stats))
                    {
                        itemsToSend.Add(stats);
                    }

                    var latestStats = etl.GetLatestPerformanceStats();
                    if (latestStats != null &&
                        latestStats.Completed == false &&
                        itemsToSend.Contains(latestStats) == false)
                        itemsToSend.Add(latestStats);

                    if (itemsToSend.Count > 0)
                    {
                        if (processesStats == null)
                            processesStats = new List<EtlProcessPerformanceStats>();
                        
                        processesStats.Add(new EtlProcessPerformanceStats
                        {
                            TransformationName = etl.Name,
                            Performance = itemsToSend.Select(item => item.ToPerformanceLiveStatsWithDetails()).ToArray()
                        });

                        type = etl.EtlType;
                    }
                }

                if (processesStats != null && processesStats.Count > 0)
                {
                    preparedStats.Add(new EtlTaskPerformanceStats
                    {
                        TaskName = taskProcesses.Key,
                        EtlType = type,
                        Stats = processesStats.ToArray()
                    });
                }
            }
            return preparedStats;
        }

        protected override void WriteStats(List<EtlTaskPerformanceStats> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteEtlTaskPerformanceStats(context, stats);
        }

        private class EtlProcessAndPerformanceStatsList : HandlerAndPerformanceStatsList<EtlProcess, EtlStatsAggregator>
        {
            public EtlProcessAndPerformanceStatsList(EtlProcess etl) : base(etl)
            {
            }
        }
    }
}
