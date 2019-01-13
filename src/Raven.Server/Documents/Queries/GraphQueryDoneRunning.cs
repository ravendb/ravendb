using System;

namespace Raven.Server.Documents.Queries
{
    public struct GraphQueryDoneRunning : IDisposable
    {
        readonly GraphQueryRunner _parent;
        private readonly ExecutingQueryInfo _queryInfo;

        public GraphQueryDoneRunning(GraphQueryRunner parent, ExecutingQueryInfo queryInfo)
        {
            _parent = parent;
            _queryInfo = queryInfo;
        }

        public void Dispose()
        {

            if (_queryInfo != null)
                _parent.Database.QueryRunner.CurrentlyRunningQueries.TryRemove(_queryInfo);
        }
    }
}
