using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<ShardedRequestHandler>
    {
        private readonly string _databaseName;

        private JsonOperationContext _context;

        private readonly DetailedDatabaseStatistics _databaseStatistics;

        private IDisposable _releaseContext;

        public ShardedStatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler, string databaseName, DetailedDatabaseStatistics databaseStatistics) : base(requestHandler)
        {
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _databaseStatistics = databaseStatistics ?? throw new ArgumentNullException(nameof(databaseStatistics));
        }

        protected override void Initialize()
        {
            _releaseContext = RequestHandler.ContextPool.AllocateOperationContext(out _context);
        }

        protected override JsonOperationContext GetContext()
        {
            return _context;
        }

        protected override string GetDatabaseName()
        {
            return _databaseName;
        }

        protected override DetailedDatabaseStatistics GetDatabaseStatistics()
        {
            return _databaseStatistics;
        }

        public override void Dispose()
        {
            base.Dispose();

            _releaseContext?.Dispose();
            _releaseContext = null;
        }
    }
}
