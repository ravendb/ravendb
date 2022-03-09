using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal class StatsHandlerProcessorForGetDatabaseStatistics : AbstractStatsHandlerProcessorForGetDatabaseStatistics<DatabaseRequestHandler>
    {
        private QueryOperationContext _context;

        private readonly DatabaseStatistics _databaseStatistics;

        public StatsHandlerProcessorForGetDatabaseStatistics([NotNull] DatabaseRequestHandler requestHandler, QueryOperationContext context, DatabaseStatistics databaseStatistics) : base(requestHandler)
        {
            _context = context;
            _databaseStatistics = databaseStatistics ?? throw new ArgumentNullException(nameof(databaseStatistics));
        }

        protected override void Initialize()
        {
        }

        protected override JsonOperationContext GetContext()
        {
            return _context.Documents;
        }

        protected override DatabaseStatistics GetDatabaseStatistics()
        {
            return _databaseStatistics;
        }

        public override void Dispose()
        {
            base.Dispose();

            _context?.Dispose();
            _context = null;
        }
    }
}
