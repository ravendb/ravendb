using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;

using Raven.Abstractions;
using Raven.Client.Data.Indexes;
using Raven.Server.Exceptions;

namespace Raven.Server.Documents.Indexes
{
    public class IndexingStatsAggregator
    {
        public readonly int Id;

        private readonly IndexingRunStats _stats;

        private IndexingStatsScope _scope;

        private volatile IndexingPerformanceStats _performanceStats;

        public IndexingStatsAggregator(int id)
        {
            Id = id;
            StartTime = SystemTime.UtcNow;
            _stats = new IndexingRunStats();
        }

        public DateTime StartTime { get; }

        public IndexingRunStats ToIndexingBatchStats()
        {
            return _stats;
        }

        public IndexingStatsScope CreateScope()
        {
            if (_scope != null)
                throw new InvalidOperationException();

            return _scope = new IndexingStatsScope(_stats);
        }

        public IndexingPerformanceBasicStats ToIndexingPerformanceLiveStats()
        {
            if (_performanceStats != null)
                return _performanceStats;

            return new IndexingPerformanceBasicStats(_scope.Duration)
            {
                Started = StartTime,
                InputCount = _stats.MapAttempts,
                SuccessCount = _stats.MapSuccesses,
                FailedCount = _stats.MapErrors,
                OutputCount = _stats.IndexingOutputs
            };
        }

        public IndexingPerformanceStats ToIndexingPerformanceStats()
        {
            if (_performanceStats != null)
                return _performanceStats;

            lock (_stats)
            {
                if (_performanceStats != null)
                    return _performanceStats;

                return _performanceStats = new IndexingPerformanceStats(_scope.Duration)
                {
                    Started = StartTime,
                    Completed = StartTime.AddMilliseconds(_scope.Duration.TotalMilliseconds),
                    Details = _scope.ToIndexingPerformanceOperation("Indexing"),
                    InputCount = _stats.MapAttempts,
                    SuccessCount = _stats.MapSuccesses,
                    FailedCount = _stats.MapErrors,
                    OutputCount = _stats.IndexingOutputs
                };
            }
        }
    }

    public class IndexingStatsScope : IDisposable
    {
        private readonly IndexingRunStats _stats;

        private Dictionary<string, IndexingStatsScope> _scopes;

        private Stopwatch _sw;

        public IndexingStatsScope(IndexingRunStats stats)
        {
            _stats = stats;
            Start();
        }

        public TimeSpan Duration => _sw.Elapsed;

        public IndexingStatsScope For(string name)
        {
            if (_scopes == null)
                _scopes = new Dictionary<string, IndexingStatsScope>(StringComparer.OrdinalIgnoreCase);

            IndexingStatsScope scope;
            if (_scopes.TryGetValue(name, out scope) == false)
                _scopes[name] = scope = new IndexingStatsScope(_stats);

            scope.Start();

            return scope;
        }

        private void Start()
        {
            if (_sw == null)
                _sw = new Stopwatch();

            _sw.Start();
        }

        public void Dispose()
        {
            _sw?.Stop();
        }

        public void AddWriteError(IndexWriteException iwe)
        {
            _stats.AddWriteError(iwe);
        }

        public void AddAnalyzerError(IndexAnalyzerException iae)
        {
            _stats.AddAnalyzerError(iae);
        }

        public void AddMapError(string key, string message)
        {
            _stats.AddMapError(key, message);
        }

        public void AddReduceError(string message)
        {
            _stats.AddReduceError(message);
        }

        public void RecordMapAttempt()
        {
            _stats.MapAttempts++;
        }

        public void RecordMapSuccess()
        {
            _stats.MapSuccesses++;
        }

        public void RecordMapError()
        {
            _stats.MapErrors++;
        }

        public void RecordIndexingOutput()
        {
            _stats.IndexingOutputs++;
        }

        public void RecordMapCompletedReason(string reason)
        {
            if (_stats.MapDetails == null)
                _stats.MapDetails = new MapRunDetails();

            _stats.MapDetails.BatchCompleteReason = reason;
        }

        public void RecordReduceTreePageModified(bool isLeaf)
        {
            if (_stats.ReduceDetails == null)
                _stats.ReduceDetails = new ReduceRunDetails();

            if (isLeaf)
                _stats.ReduceDetails.NumberOfModifiedLeafs++;
            else
                _stats.ReduceDetails.NumberOfModifiedBranches++;
        }

        public void RecordReduceAttempts(int numberOfEntries)
        {
            _stats.ReduceAttempts += numberOfEntries;
        }

        public void RecordReduceSuccesses(int numberOfEntries)
        {
            _stats.ReduceSuccesses += numberOfEntries;
        }

        public void RecordReduceErrors(int numberOfEntries)
        {
            _stats.ReduceErrors += numberOfEntries;
        }

        public IndexingPerformanceOperation ToIndexingPerformanceOperation(string name)
        {
            var operation = new IndexingPerformanceOperation(_sw.Elapsed)
            {
                Name = name
            };

            if (_stats.ReduceDetails != null && name == IndexingOperation.Reduce.TreeScope)
                operation.Details = _stats.ReduceDetails;

            if (_stats.MapDetails != null && name == "Map")
                operation.Details = _stats.MapDetails;

            if (_scopes != null)
            {
                operation.Operations = _scopes
                    .Select(x => x.Value.ToIndexingPerformanceOperation(x.Key))
                    .ToArray();
            }

            return operation;
        }

        public void RecordMapMemoryStats(long currentProcessWorkingSet, long currentProcessPrivateMemorySize, long currentlyAllocated, long currentBudget)
        {
            if (_stats.MapDetails == null)
                _stats.MapDetails = new MapRunDetails();

            _stats.MapDetails.AllocationBudget = currentBudget;
            _stats.MapDetails.ProcessPrivateMemory = currentProcessPrivateMemorySize;
            _stats.MapDetails.ProcessWorkingSet = currentProcessWorkingSet;
            _stats.MapDetails.CurrentlyAllocated = currentlyAllocated;

        }
    }
}