using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
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

        protected override async ValueTask<(TimeSeriesRangeResult Result, long? TotalResults, HttpStatusCode StatusCode)> GetTimeSeriesAsync(TransactionOperationContext context, string docId, string name, DateTime @from, DateTime to, int start, int pageSize, bool includeDoc,
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
            var result = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber);
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle status codes. RavenDB-18416");
            return (result, result?.TotalResults, cmd.StatusCode);
        }
    }
}
