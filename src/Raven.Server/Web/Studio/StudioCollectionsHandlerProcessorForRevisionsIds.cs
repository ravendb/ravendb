using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Documents;
using Raven.Server.Documents.Handlers.Processors;
using Raven.Server.Documents.Schemas;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web.Studio.Processors;
using Sparrow.Json;

namespace Raven.Server.Web.Studio;

internal sealed class StudioCollectionsHandlerProcessorForRevisionsIds : AbstractStudioCollectionsHandlerProcessorForRevisionsIds<DatabaseRequestHandler, DocumentsOperationContext>
{
    public StudioCollectionsHandlerProcessorForRevisionsIds([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    protected override IDisposable OpenReadTransaction(DocumentsOperationContext context) => context.OpenReadTransaction();

    protected override Task WriteItemsAsync(DocumentsOperationContext context, AsyncBlittableJsonTextWriter writer, CancellationToken token)
    {
        writer.WriteStartArray();
        var first = true;
        foreach (var id in RequestHandler.Database.DocumentsStorage.RevisionsStorage.GetRevisionsIdsByPrefix(context, Prefix, PageSize))
        {
            if (first)
                first = false;
            else
                writer.WriteComma();

            writer.WriteStartObject();

            writer.WritePropertyName(nameof(Document.Id));
            writer.WriteString(id);

            writer.WriteEndObject();
        }
        writer.WriteEndArray();
        return Task.CompletedTask;
    }
}

