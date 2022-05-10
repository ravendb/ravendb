using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading.Tasks;
using Amazon.Glacier;
using JetBrains.Annotations;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Client.Extensions;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Utils;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetTimeSeries : AbstractTimeSeriesHandlerProcessorForGetTimeSeries<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetTimeSeries([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask<TimeSeriesRangeResult> GetTimeSeriesAndWriteAsync(TransactionOperationContext context, string docId, string name, DateTime @from, DateTime to, int start, int pageSize, bool includeDoc,
            bool includeTags, bool fullResults)
        {
            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, docId);
            
            Action<ITimeSeriesIncludeBuilder> action = includeBuilder =>
            {
                if (includeDoc)
                    includeBuilder.IncludeDocument();
                if (includeTags)
                    includeBuilder.IncludeTags();
            };
            
            var cmd = new GetTimeSeriesOperation.GetTimeSeriesCommand(docId, name, from, to, start, pageSize, action, fullResults);
            var rangeResult = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(context, cmd, shardNumber);

            if (rangeResult == null)
            {
                if(cmd.StatusCode == HttpStatusCode.NotFound)
                    RequestHandler.HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                return null;
            }

            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle not modified for include tags");

            var nonLocalIncludes = new List<string>();
            foreach (var id in rangeResult.MissingIncludes)
            {
                if(RequestHandler.DatabaseContext.GetShardNumber(context, id) != shardNumber)
                    nonLocalIncludes.Add(id);
            }

            var idsByShards = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, nonLocalIncludes);
            var fetchDocsOp = new FetchDocumentsFromShardsOperation(context, RequestHandler, idsByShards, etag: null, includePaths: null, metadataOnly: false);

            ShardedReadResult<GetShardedDocumentsResult> result;
            using (var token = RequestHandler.CreateOperationToken())
                result = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(idsByShards.Keys.ToArray(), fetchDocsOp, token.Token);
            
            var includesDocId = $"TimeSeriesRangeIncludes/{docId}";
            using (context.OpenReadTransaction())
            {
                rangeResult.Includes ??= context.ReadObject(new DynamicJsonValue(), includesDocId);

                var mods = new DynamicJsonValue(rangeResult.Includes);
                foreach (var (id, data) in result.Result.Documents)
                {
                    rangeResult.MissingIncludes.Remove(id);
                    mods[id] = data;
                }

                rangeResult.Includes.Modifications = mods;
                rangeResult.Includes = context.ReadObject(rangeResult.Includes, includesDocId);
            }
            
            return rangeResult;
        }
    }
}
