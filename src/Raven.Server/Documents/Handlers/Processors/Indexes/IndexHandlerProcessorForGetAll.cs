﻿using System.Linq;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Indexes;
using Raven.Server.Json;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Http;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers.Processors.Indexes;

internal class IndexHandlerProcessorForGetAll : AbstractIndexHandlerProcessorForGetAll<DatabaseRequestHandler, DocumentsOperationContext>
{
    public IndexHandlerProcessorForGetAll([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override bool SupportsCurrentNode => true;

    protected override ValueTask HandleCurrentNodeAsync()
    {
        var name = GetName();
        var start = RequestHandler.GetStart();
        var pageSize = RequestHandler.GetPageSize();

        var indexDefinitions = GetIndexDefinitions(RequestHandler, name, start, pageSize);

        return WriteResultAsync(indexDefinitions);
    }

    protected override Task HandleRemoteNodeAsync(ProxyCommand<IndexDefinition[]> command, OperationCancelToken token) => RequestHandler.ExecuteRemoteAsync(command, token.Token);

    internal static IndexDefinition[] GetIndexDefinitions(DatabaseRequestHandler requestHandler, string indexName, int start, int pageSize)
    {
        IndexDefinition[] indexDefinitions;
        if (string.IsNullOrEmpty(indexName))
            indexDefinitions = requestHandler.Database.IndexStore
                .GetIndexes()
                .OrderBy(x => x.Name)
                .Skip(start)
                .Take(pageSize)
                .Select(x => x.GetIndexDefinition())
                .ToArray();
        else
        {
            var index = requestHandler.Database.IndexStore.GetIndex(indexName);
            if (index == null)
                return null;

            indexDefinitions = new[] { index.GetIndexDefinition() };
        }

        return indexDefinitions;
    }

    private async ValueTask WriteResultAsync(IndexDefinition[] result)
    {
        if (result == null)
        {
            HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
            return;
        }

        using (ContextPool.AllocateOperationContext(out JsonOperationContext context))
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WriteArray(context, "Results", result, (w, c, indexDefinition) =>
            {
                w.WriteIndexDefinition(c, indexDefinition);
            });

            writer.WriteEndObject();
        }
    }
}
