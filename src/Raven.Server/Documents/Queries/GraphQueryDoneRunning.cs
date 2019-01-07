using System;

namespace Raven.Server.Documents.Queries
{
    public struct GraphQueryDoneRunning : IDisposable
    {
        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromSeconds(3);

        private static readonly TimeSpan ExtendedLockTimeout = TimeSpan.FromSeconds(30);

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
                _parent.Database.RemoveFromCurrentlyRunningGraphQueries(_queryInfo);
        }
    }
}
