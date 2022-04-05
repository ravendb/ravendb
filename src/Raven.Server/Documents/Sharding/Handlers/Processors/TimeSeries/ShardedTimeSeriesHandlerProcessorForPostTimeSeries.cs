using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Exceptions.Documents;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.ServerWide.Context;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForPostTimeSeries : AbstractTimeSeriesHandlerProcessorForPostTimeSeries<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForPostTimeSeries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler, requestHandler.ContextPool)
        {
        }

        protected override async ValueTask ApplyTimeSeriesOperationAsync(string docId, TimeSeriesOperation operation, TransactionOperationContext context)
        {
            int shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            using (var token = RequestHandler.CreateOperationToken())
            {
                var cmd = new TimeSeriesBatchOperation.TimeSeriesBatchCommand(docId, operation);
                await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(cmd, shardNumber, token.Token);

                switch (cmd.StatusCode)
                {
                    case HttpStatusCode.NotFound:
                        HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                        DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Arek, DevelopmentHelper.Severity.Normal, "Execution of command should rethrow the exception using injected behavior");
                        throw new DocumentDoesNotExistException(docId);
                    case HttpStatusCode.NoContent:
                        break;
                    default:
                        throw new ArgumentOutOfRangeException(nameof(cmd.StatusCode), $"Not supported status code: {cmd.StatusCode}");
                }
            }
        }
    }
}
