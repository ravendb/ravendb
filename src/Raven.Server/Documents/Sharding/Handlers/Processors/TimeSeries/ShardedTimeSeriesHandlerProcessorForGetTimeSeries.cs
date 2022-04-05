using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Sharding.Commands;
using Raven.Server.Exceptions;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetTimeSeries : AbstractTimeSeriesHandlerProcessorForGetTimeSeries<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetTimeSeries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask GetTimeSeriesAndWriteToStreamAsync(TransactionOperationContext context, string docId, string name, DateTime @from, DateTime to, int start, int pageSize, bool includeDoc,
            bool includeTags, bool fullResults)
        {
            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);

            if (includeTags)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Support include tags in time series?");
                throw new NotSupportedInShardingException("Include tags of time series is not supported in sharding");
            }

            var cmd = new GetRawTimeSeriesCommand(RequestHandler, docId, name, from, to, start, pageSize, includeDoc, includeTags, fullResults);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber);
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle status codes. RavenDB-18416");
        }
    }
}
