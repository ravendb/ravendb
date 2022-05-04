using System;
using System.Collections.Generic;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes.Debugging;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForDebug : AbstractIndexHandlerProcessorForDebug<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForDebug([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override async ValueTask HandleCurrentNodeAsync()
    {
        var name = GetIndexName();

        var index = RequestHandler.Database.IndexStore.GetIndex(name);
        if (index == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        var operation = GetOperation();

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            if (string.Equals(operation, "map-reduce-tree", StringComparison.OrdinalIgnoreCase))
            {
                if (index.Type.IsMapReduce() == false)
                {
                    HttpContext.Response.StatusCode = (int)HttpStatusCode.BadRequest;

                    context.Write(writer, new DynamicJsonValue
                    {
                        ["Error"] = $"{index.Name} is not map-reduce index"
                    });

                    return;
                }

                var docIds = GetDocIds();

                using (index.GetReduceTree(docIds.ToArray(), out IEnumerable<ReduceTree> trees))
                {
                    writer.WriteReduceTrees(trees);
                }

                return;
            }

            if (string.Equals(operation, "source-doc-ids", StringComparison.OrdinalIgnoreCase))
            {
                using (index.GetIdentifiersOfMappedDocuments(GetStartsWith(), RequestHandler.GetStart(), RequestHandler.GetPageSize(), out IEnumerable<string> ids))
                {
                    writer.WriteArrayOfResultsAndCount(ids);
                }

                return;
            }

            if (string.Equals(operation, "entries-fields", StringComparison.OrdinalIgnoreCase))
            {
                var fields = index.GetEntriesFields();

                writer.WriteStartObject();

                writer.WriteArray(nameof(fields.Static), fields.Static);
                writer.WriteComma();

                writer.WriteArray(nameof(fields.Dynamic), fields.Dynamic);

                writer.WriteEndObject();

                return;
            }

            throw new NotSupportedException($"{operation} is not supported");
        }
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<object> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);
}
