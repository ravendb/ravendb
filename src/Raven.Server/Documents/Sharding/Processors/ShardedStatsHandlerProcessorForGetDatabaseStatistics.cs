using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Handlers.Processors;
using Sparrow.Json;

namespace Raven.Server.Documents.Sharding.Processors
{
    internal class ShardedStatsHandlerProcessorForGetDatabaseStatistics : AbstractStatsHandlerProcessorForGetDatabaseStatistics <ShardedRequestHandler>
    {
        private JsonOperationContext _context;

        private readonly DatabaseStatistics _databaseStatistics;

        private IDisposable _releaseContext;

        public ShardedStatsHandlerProcessorForGetDatabaseStatistics([NotNull] ShardedRequestHandler requestHandler, DatabaseStatistics databaseStatistics) : base(requestHandler)
        {
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

        protected override DatabaseStatistics GetDatabaseStatistics()
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
