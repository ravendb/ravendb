using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.Extensions.Primitives;
using Raven.Client;
using Raven.Client.Documents.Operations.TimeSeries;
using Raven.Client.Documents.Session.Loaders;
using Raven.Server.Documents.Handlers.Processors.TimeSeries;
using Raven.Server.Documents.Sharding.Operations;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Utils;

namespace Raven.Server.Documents.Sharding.Handlers.Processors.TimeSeries
{
    internal class ShardedTimeSeriesHandlerProcessorForGetTimeSeriesRanges : AbstractTimeSeriesHandlerProcessorForGetTimeSeriesRanges<ShardedDatabaseRequestHandler, TransactionOperationContext>
    {
        public ShardedTimeSeriesHandlerProcessorForGetTimeSeriesRanges([NotNull] ShardedDatabaseRequestHandler requestHandler) : base(requestHandler)
        {
        }

        protected override async ValueTask GetTimeSeriesRangesAndWriteAsync(TransactionOperationContext context, string documentId, StringValues names, StringValues fromList, StringValues toList, int start,
            int pageSize, bool includeDoc, bool includeTags, bool returnFullResults, CancellationToken token)
        {
            var shardNumber = RequestHandler.DatabaseContext.GetShardNumber(context, documentId);

            Action<ITimeSeriesIncludeBuilder> action = includeBuilder =>
            {
                if (includeDoc)
                    includeBuilder.IncludeDocument();
                if (includeTags)
                    includeBuilder.IncludeTags();
            };

            var requestedRanges = ConvertAndValidateMultipleTimeSeriesParameters(documentId, names, fromList, toList);
            var cmd = new ShardedGetMultipleTimeSeriesCommand(documentId, requestedRanges, start, pageSize, action);
            cmd.ModifyRequest = r => r.Headers.TryAddWithoutValidation(Constants.Headers.Sharded, "true");
            var rangesResult = await RequestHandler.ShardExecutor.ExecuteSingleShardAsync(context, cmd, shardNumber, token);
            
            if (rangesResult == null)
            {
                if (cmd.StatusCode == HttpStatusCode.NotFound)
                    RequestHandler.HttpContext.Response.StatusCode = (int)cmd.StatusCode;
                return;
            }
            
            DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Stav, DevelopmentHelper.Severity.Normal, "Handle not modified for include tags and its status code");

            if (includeDoc || includeTags)
            {
                // rangeResultToIncludes contains object to all its missing docs, but all doc ids will appear overall once over all ranges.
                // the docs are saved in the session cache and there is no sense in writing them multiple times in the response for each range
                var rangeResultToIncludes = new Dictionary<TimeSeriesRangeResult, HashSet<string>>();
                var nonLocalMissingIncludes = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                foreach (var (name, ranges) in rangesResult.Values)
                {
                    foreach (var range in ranges)
                    {
                        foreach (var id in range.MissingIncludes)
                        {
                            if (RequestHandler.DatabaseContext.GetShardNumber(context, id) != shardNumber && nonLocalMissingIncludes.Contains(id) == false)
                            {
                                if (rangeResultToIncludes.ContainsKey(range) == false)
                                    rangeResultToIncludes[range] = new HashSet<string>(StringComparer.OrdinalIgnoreCase) {id};
                                else
                                    rangeResultToIncludes[range].Add(id);
                                nonLocalMissingIncludes.Add(id);
                            }
                        }
                    }
                }

                var idsByShards = ShardLocator.GetDocumentIdsByShards(context, RequestHandler.DatabaseContext, nonLocalMissingIncludes);
                var fetchDocsOp = new FetchDocumentsFromShardsOperation(context, RequestHandler, idsByShards, etag: null, includePaths: null, metadataOnly: false);
                var result = await RequestHandler.ShardExecutor.ExecuteParallelForShardsAsync(idsByShards.Keys.ToArray(), fetchDocsOp, token);

                var includesDocId = $"TimeSeriesRangeIncludes/{documentId}";

                foreach (var (rangeResult, includesHs) in rangeResultToIncludes)
                {
                    rangeResult.Includes ??= context.ReadObject(new DynamicJsonValue(), includesDocId);
                    var mods = new DynamicJsonValue(rangeResult.Includes);
                    rangeResult.MissingIncludes = null;

                    foreach (var docId in includesHs)
                    {
                        if (result.Result.Documents.ContainsKey(docId))
                        {
                            mods[docId] = result.Result.Documents[docId];
                        }
                    }

                    rangeResult.Includes.Modifications = mods;
                    rangeResult.Includes = context.ReadObject(rangeResult.Includes, includesDocId);
                }
            }

            await WriteTimeSeriesDetails(writeContext: context, docsContext: null, documentId: null, rangesResult, token);
        }

        internal class ShardedGetMultipleTimeSeriesCommand : GetMultipleTimeSeriesOperation.GetMultipleTimeSeriesCommand
        {
            public ShardedGetMultipleTimeSeriesCommand(string docId, IEnumerable<TimeSeriesRange> ranges, int start, int pageSize, Action<ITimeSeriesIncludeBuilder> includes = null, bool returnFullResults = false) : base(docId, ranges, start, pageSize, includes, returnFullResults)
            {
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                base.SetResponse(context, response, fromCache);
                foreach (var (name, rangeResults) in Result.Values)
                {
                    if (response.TryGet(nameof(Result.Values), out BlittableJsonReaderObject values) &&
                        values.TryGet(name, out BlittableJsonReaderArray ranges))
                    {
                        int j = 0;
                        foreach (BlittableJsonReaderObject range in ranges)
                        {
                            AddInternalFieldsToResultForSharded(rangeResults[j], range);
                            j++;
                        }
                    }
                }
            }
        }
    }
}
