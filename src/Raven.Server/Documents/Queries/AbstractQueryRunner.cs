using System;
using System.Collections.Generic;
using System.Threading;
using Raven.Server.ServerWide;
using Sparrow.Collections;

namespace Raven.Server.Documents.Queries
{
    public abstract class AbstractQueryRunner
    {
        public const string AllDocsCollectionName = "AllDocs";

        public const string CollectionIndexPrefix = "collection/";

        public const string DynamicIndexPrefix = "dynamic/";

        private long _nextQueryId;

        private readonly ConcurrentSet<ExecutingQueryInfo> _currentlyRunningQueries;

        protected AbstractQueryRunner()
        {
            _currentlyRunningQueries = new ConcurrentSet<ExecutingQueryInfo>();
        }

        public IEnumerable<ExecutingQueryInfo> CurrentlyRunningQueries => _currentlyRunningQueries;

        public QueryMarker MarkQueryAsRunning(string name, IndexQueryServerSide query, OperationCancelToken token, bool isStreaming = false)
        {
            var queryStartTime = DateTime.UtcNow;
            var queryId = Interlocked.Increment(ref _nextQueryId);

            var executingQueryInfo = new ExecutingQueryInfo(queryStartTime, name, query, queryId, isStreaming, token);

            _currentlyRunningQueries.TryAdd(executingQueryInfo);

            return new QueryMarker(this, executingQueryInfo);
        }

        public static string GetIndexName(IndexQueryServerSide query)
        {
            if (query.Metadata.IsCollectionQuery)
            {
                var collection = query.Metadata.CollectionName;
                return string.IsNullOrEmpty(collection) 
                    ? AllDocsCollectionName 
                    : $"{CollectionIndexPrefix}{collection}";
            }

            return query.Metadata.IsDynamic
                ? $"{DynamicIndexPrefix}/{query.Metadata.CollectionName}"
                : query.Metadata.IndexName;
        }

        public struct QueryMarker : IDisposable
        {
            private readonly AbstractQueryRunner _queryRunner;

            private readonly ExecutingQueryInfo _queryInfo;

            public readonly DateTime StartTime;

            public long QueryId;

            public QueryMarker(AbstractQueryRunner queryRunner, ExecutingQueryInfo queryInfo)
            {
                _queryRunner = queryRunner;
                _queryInfo = queryInfo;

                StartTime = queryInfo.StartTime;
                QueryId = queryInfo.QueryId;
            }

            public void Dispose()
            {
                if (_queryInfo != null)
                    _queryRunner._currentlyRunningQueries.TryRemove(_queryInfo);
            }
        }
    }
}
