using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Http;
using Raven.Client.Http;
using Raven.Server.Documents.Handlers.Processors.Stats;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils.Metrics.Commands;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.Stats
{
    internal class ShardedStatsHandlerProcessorForGetMetrics : AbstractStatsHandlerProcessorForGetMetrics<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedStatsHandlerProcessorForGetMetrics([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<DynamicJsonValue> GetDatabaseMetricsAsync(JsonOperationContext context)
        {
            using (var token = RequestHandler.CreateOperationToken())
            {
                var metrics = await RequestHandler.ShardExecutor.ExecuteParallelForAllAsync(new GetShardedDatabaseMetricsOperation(RequestHandler, context, puts: null, bytes: null), token.Token);
                return metrics;
            }
        }

        internal readonly struct GetShardedDatabaseMetricsOperation : IShardedOperation<BlittableJsonReaderObject, DynamicJsonValue>
        {
            private readonly ShardedDatabaseRequestHandler _handler;
            private readonly JsonOperationContext _context;
            private readonly bool? _puts;
            private readonly bool? _bytes;

            public GetShardedDatabaseMetricsOperation(ShardedDatabaseRequestHandler handler, JsonOperationContext context, bool? puts, bool? bytes)
            {
                _handler = handler;
                _context = context;
                _puts = puts;
                _bytes = bytes;
            }

            public HttpRequest HttpRequest => _handler.HttpContext.Request;

            public DynamicJsonValue Combine(Memory<BlittableJsonReaderObject> results)
            {
                var combined = new DynamicJsonValue();

                for (var i = 0; i < results.Span.Length; i++)
                {
                    var result = results.Span[i];
                    var databaseName = $"{_handler.DatabaseName}${i}";
                    combined[databaseName] = result.Clone(_context);
                }

                return combined;
            }

            public RavenCommand<BlittableJsonReaderObject> CreateCommandForShard(int shardNumber)
            {
                var empty = _handler.GetBoolValueQueryString("empty", required: false) ?? true;
                if (_puts.HasValue)
                    return new GetDatabaseMetricsCommand(putsOnly: true, bytesOnly: false, filterEmpty: empty);
                if (_bytes.HasValue)
                    return new GetDatabaseMetricsCommand(putsOnly: false, bytesOnly: true, filterEmpty: empty);

                return new GetDatabaseMetricsCommand(putsOnly: false, bytesOnly: false);
            }
        }
    }
}
