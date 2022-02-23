using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using System.Web;
using NuGet.Packaging;
using Raven.Client.Documents.Commands;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Session.Operations;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Json.Serialization;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.ShardedHandlers;
using Raven.Server.Documents.ShardedHandlers.ShardedCommands;
using Raven.Server.Documents.Sharding;
using Raven.Server.Json;
using Raven.Server.Routing;
using Sparrow.Json;
using Sparrow.Utils;

namespace Raven.Server.Web.Studio.Sharding
{
    public class ShardedStudioCollectionsHandler : ShardedRequestHandler
    {
        [RavenShardedAction("/databases/*/studio/collections/preview", "GET")]
        public async Task PreviewCollection()
        {
            using (Server.ServerStore.ContextPool.AllocateOperationContext(out JsonOperationContext context))
            {
                var collection = GetStringQueryString("collection", required: false);
                var bindings = GetStringValuesQueryString("binding", required: false);
                var fullBindings = GetStringValuesQueryString("fullBinding", required: false);

                var token = GetOrCreateContinuationToken(context);
                var op = new ShardedCollectionPreviewOperation(this, token, context);
                var result = await ShardExecutor.ExecuteParallelForAllAsync(op);

                DevelopmentHelper.ShardingToDo(DevelopmentHelper.TeamMember.Karmel, DevelopmentHelper.Severity.Normal, "Need to figure out the best way to combine ETags and send not modified");

                var documents = result.Results;
                var availableColumns = result.AvailableColumns;
                var fullPropertiesToSend = new HashSet<string>(fullBindings);

                HashSet<string> propertiesPreviewToSend;
                if (string.IsNullOrEmpty(collection))
                {
                    propertiesPreviewToSend = bindings.Count > 0 ? new HashSet<string>(bindings) : new HashSet<string>();
                }
                else
                {
                    propertiesPreviewToSend = bindings.Count > 0 ? new HashSet<string>(bindings) : availableColumns.Take(StudioCollectionsHandler.ColumnsSamplingLimit).Select(x => x.ToString()).ToHashSet();
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();
                    writer.WritePropertyName(nameof(PreviewCollectionResult.Results));

                    writer.WriteStartArray();

                    var first = true;
                    foreach (var document in documents)
                    {
                        if (first == false)
                            writer.WriteComma();
                        first = false;

                        using (document.Data)
                        {
                            StudioCollectionsHandler.WriteDocument(writer, context, document, propertiesPreviewToSend, fullPropertiesToSend);
                        }
                    }

                    writer.WriteEndArray();

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(PreviewCollectionResult.TotalResults));
                    writer.WriteInteger(result.TotalResults);

                    writer.WriteComma();

                    writer.WriteArray(nameof(PreviewCollectionResult.AvailableColumns), availableColumns);
                    writer.WriteComma();

                    writer.WriteContinuationToken(context, token);
                    writer.WriteEndObject();
                }
            }
        }

        private readonly struct ShardedCollectionPreviewOperation : IShardedOperation<PreviewCollectionResult>
        {
            private readonly ShardedRequestHandler _handler;
            private readonly ShardedPagingContinuation _token;
            private readonly JsonOperationContext _context;

            public ShardedCollectionPreviewOperation(ShardedRequestHandler handler, ShardedPagingContinuation token, JsonOperationContext context)
            {
                _handler = handler;
                _token = token;
                _context = context;
            }

            public PreviewCollectionResult Combine(Memory<PreviewCollectionResult> results)
            {
                var availableColumns = new HashSet<string>(StringComparer.InvariantCultureIgnoreCase);
                var total = new PreviewCollectionResult
                {
                    Results = new List<Document>()
                };

                var span = results.Span;
                var totalDocuments = 0L;

                for (int i = 0; i < span.Length; i++)
                {
                    availableColumns.AddRange(span[i].AvailableColumns);
                    total.TotalResults += span[i].TotalResults;
                }

                for (int i = 0; i < span.Length * _token.PageSize; i++)
                {
                    var takeFromShard = i % _token.Pages.Length;
                    
                    var shardDocs = span[takeFromShard].Results;
                    if (shardDocs.Count == 0)
                        continue;

                    if (totalDocuments++ >= _token.PageSize)
                        return total;

                    var position = shardDocs.Count - 1;
                    var doc = shardDocs[^1].Clone(_context);
                    shardDocs.RemoveAt(position);
                    _token.Pages[takeFromShard].Start++;
                    total.Results.Add(doc);
                }

                total.AvailableColumns = availableColumns.ToList();
                total.Results.Sort(DocumentByLastModifiedComparer.Instance);
                return total;
            }

            public RavenCommand<PreviewCollectionResult> CreateCommandForShard(int shard) =>
                new ShardedCollectionPreviewCommand(_handler, _token.Pages[shard].Start, _token.PageSize);
        }


        private class ShardedCollectionPreviewCommand : ShardedBaseCommand<PreviewCollectionResult>
        {
            public override bool IsReadRequest => true;

            public ShardedCollectionPreviewCommand(ShardedRequestHandler handler, int start, int pageSize) : base(handler, Documents.ShardedHandlers.ShardedCommands.Headers.IfNoneMatch, content: null)
            {
                var queryString = HttpUtility.ParseQueryString(handler.HttpContext.Request.QueryString.Value);
                queryString[StartParameter] = start.ToString();
                queryString[PageSizeParameter] = pageSize.ToString();
                Url = handler.BaseShardUrl + "?" + queryString;
            }

            public override void SetResponse(JsonOperationContext context, BlittableJsonReaderObject response, bool fromCache)
            {
                if (response == null)
                    ThrowInvalidResponse();

                var result = new PreviewCollectionResult
                {
                    Results = new List<Document>(),
                    AvailableColumns = new List<string>()
                };

                response.TryGet(nameof(PreviewCollectionResult.Results), out BlittableJsonReaderArray array);
                response.TryGet(nameof(PreviewCollectionResult.TotalResults), out result.TotalResults);
                response.TryGet(nameof(PreviewCollectionResult.AvailableColumns), out BlittableJsonReaderArray availableColumns);

                foreach (BlittableJsonReaderObject doc in array)
                {
                    var metadata = doc.GetMetadata();

                    result.Results.Add(new Document
                    {
                        Data = doc,
                        ChangeVector = metadata.GetChangeVector(),
                        LastModified = metadata.GetLastModified(),
                        Id = metadata.GetLazyStringId(),
                    });
                }

                foreach (LazyStringValue column in availableColumns)
                {
                    result.AvailableColumns.Add(column);
                }

                Result = result;
            }
        }

        public class PreviewCollectionResult
        {
            public List<Document> Results;
            public long TotalResults;
            public List<string> AvailableColumns;
        }
    }
}
