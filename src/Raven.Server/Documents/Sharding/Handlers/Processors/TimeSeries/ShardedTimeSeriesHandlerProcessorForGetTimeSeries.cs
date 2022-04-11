using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Documents.Session.TimeSeries;
using Raven.Client.Exceptions.Sharding;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetTimeSeries : AbstractTimeSeriesHandlerProcessorForGetTimeSeries<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetTimeSeries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask GetTimeSeriesAsync(TransactionOperationContext context, string docId, string name, DateTime @from, DateTime to, int start, int pageSize, bool includeDoc,
            bool includeTags, bool fullResults)
        {
            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);

            if (includeTags)
            {
                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Support include tags in time series?");
                throw new NotSupportedInShardingException("Include tags of time series is not supported in sharding");
            }

            Action<ITimeSeriesIncludeBuilder> builder = null;
            if (includeDoc)
                builder = bldr => bldr.IncludeDocument();

            var cmd = new GetTimeSeriesOperation.GetTimeSeriesCommand(docId, name, from, to, start, pageSize, builder, fullResults);
            var proxy = new ProxyCommand<TimeSeriesRangeResult<TimeSeriesEntry>>(cmd, RequestHandler.HttpContext.Response);
            await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(proxy, shardNumber);
        }
    }
}
