using System;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Indexes;
using Raven.Server.Utils;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors
{
    internal class StatsHandlerProcessorForGetDetailedDatabaseStatistics : AbstractStatsHandlerProcessorForGetDetailedDatabaseStatistics<DatabaseRequestHandler>
    {
        private readonly string _databaseName;

        private QueryOperationContext _context;

        private readonly DetailedDatabaseStatistics _databaseStatistics;

        public StatsHandlerProcessorForGetDetailedDatabaseStatistics([NotNull] DatabaseRequestHandler requestHandler, string databaseName, QueryOperationContext context, DetailedDatabaseStatistics databaseStatistics) : base(requestHandler)
        {
            _context = context;
            _databaseName = databaseName ?? throw new ArgumentNullException(nameof(databaseName));
            _databaseStatistics = databaseStatistics ?? throw new ArgumentNullException(nameof(databaseStatistics));
        }

        protected override void Initialize()
        {
        }

        protected override JsonOperationContext GetContext()
        {
            return _context.Documents;
        }

        protected override string GetDatabaseName()
        {
            return ShardHelper.ToDatabaseName(_databaseName);
        }

        protected override DetailedDatabaseStatistics GetDatabaseStatistics()
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
