using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Server.ServerWide;
using Raven.Server.Utils.Stats;
using Sparrow.Json;

namespace Raven.Server.Documents.Queries
{
    public class LiveRunningQueriesCollector : LivePerformanceCollector<LiveRunningQueriesCollector.ExecutingQueryCollection>
    {
        private readonly ServerStore _serverStore;
        private readonly HashSet<string> _dbNames;
        
        public LiveRunningQueriesCollector(ServerStore serverStore, HashSet<string> dbNames)
            : base(serverStore.ServerShutdown, "Server")
        {
            _dbNames = dbNames;
            _serverStore = serverStore;
            
            Start();
        }

        protected override TimeSpan SleepTime => TimeSpan.FromSeconds(1);

        protected override bool ShouldEnqueue(List<ExecutingQueryCollection> items)
        {
            // always enqueue new message
            return true;
        }

        protected override async Task StartCollectingStats()
        {
            var stats = PreparePerformanceStats();
            Stats.Enqueue(stats);

            await RunInLoop();
        }

        protected override List<ExecutingQueryCollection> PreparePerformanceStats()
        {
            var result = new List<ExecutingQueryCollection>();
            
            foreach ((var dbName, Task<DocumentDatabase> value) in _serverStore.DatabasesLandlord.DatabasesCache)
            {
                if (value.IsCompletedSuccessfully == false)
                    continue;

                var dbNameAsString = dbName.ToString();
                
                if (_dbNames != null && !_dbNames.Contains(dbNameAsString))
                    continue;

                var database = value.Result;
                
                var indexes = database
                    .IndexStore
                    .GetIndexes()
                    .ToList();
                        
                foreach (var index in indexes)
                {
                    var runningQueries = index.CurrentlyRunningQueries
                        .Where(x => x.DurationInMs > 100)
                        .ToList();

                    if (runningQueries.Count == 0)
                    {
                        continue;
                    }
                            
                    result.Add(new ExecutingQueryCollection
                    {
                        DatabaseName = dbNameAsString,
                        IndexName = index.Name,
                        RunningQueries = runningQueries
                    });
                }
            }
            
            return result;
        }

        protected override void WriteStats(List<ExecutingQueryCollection> stats, AsyncBlittableJsonTextWriter writer, JsonOperationContext context)
        {
            writer.WriteStartArray();

            var isFirst = true;
            
            foreach (var executingQueryCollection in stats)
            {
                if (isFirst == false)
                {
                    writer.WriteComma();
                }
                
                writer.WriteStartObject();

                isFirst = false;
                writer.WritePropertyName(nameof(executingQueryCollection.DatabaseName));
                writer.WriteString(executingQueryCollection.DatabaseName);
                writer.WriteComma();
                
                writer.WritePropertyName(nameof(executingQueryCollection.IndexName));
                writer.WriteString(executingQueryCollection.IndexName);
                writer.WriteComma();
                
                writer.WritePropertyName(nameof(executingQueryCollection.RunningQueries));
                writer.WriteStartArray();

                var firstInnerQuery = true; 
                foreach (var executingQueryInfo in executingQueryCollection.RunningQueries)
                {
                    if (firstInnerQuery == false)
                        writer.WriteComma();

                    firstInnerQuery = false;
                    executingQueryInfo.Write(writer, context);
                }
                writer.WriteEndArray();
                
                writer.WriteEndObject();
                
            }
            
            writer.WriteEndArray();
        }

        public class ExecutingQueryCollection
        {
            public string DatabaseName { get; set; }
            public string IndexName { get; set; }
            public List<ExecutingQueryInfo> RunningQueries { get; set; }
        }
        
    }
}
