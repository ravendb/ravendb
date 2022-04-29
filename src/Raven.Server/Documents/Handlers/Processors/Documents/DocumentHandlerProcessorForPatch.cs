using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers.Processors.Documents;

internal class DocumentHandlerProcessorForPatch : AbstractDocumentHandlerProcessorForPatch<DocumentHandler, DocumentsOperationContext>
{
    public DocumentHandlerProcessorForPatch([NotNull] DocumentHandler requestHandler) : base(requestHandler)
    {
    }

    protected override async ValueTask HandleDocumentPatchAsync(string id, string cv, BlittableJsonReaderObject patchRequest, bool skipPatchIfChangeVectorMismatch, bool debugMode,
        bool isTest, DocumentsOperationContext context)
    {
        var changeVector = context.GetLazyString(cv);

        patchRequest.TryGet("Patch", out BlittableJsonReaderObject patchCmd);
        patchRequest.TryGet("PatchIfMissing", out BlittableJsonReaderObject patchIfMissingCmd);
        patchRequest.TryGet("CreateIfMissing", out BlittableJsonReaderObject createIfMissingCmd);

        var patch = PatchRequest.Parse(patchCmd, out var patchArgs);

        PatchRequest patchIfMissing = null;
        BlittableJsonReaderObject patchIfMissingArgs = null;

        if (patchIfMissingCmd != null)
            patchIfMissing = PatchRequest.Parse(patchIfMissingCmd, out patchIfMissingArgs);

        var command = new PatchDocumentCommand(context,
            id,
            changeVector,
            skipPatchIfChangeVectorMismatch,
            (patch, patchArgs),
            (patchIfMissing, patchIfMissingArgs),
            createIfMissingCmd,
            RequestHandler.Database.IdentityPartsSeparator,
            isTest,
            debugMode,
            true,
            returnDocument: false
        );

        if (isTest == false)
        {
            await RequestHandler.Database.TxMerger.Enqueue(command);
        }
        else
        {
            // PutDocument requires the write access to the docs storage
            // testing patching is rare enough not to optimize it
            using (context.OpenWriteTransaction())
            {
                command.Execute(context, null);
            }
        }

        switch (command.PatchResult.Status)
        {
            case PatchStatus.DocumentDoesNotExist:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotFound;
                return;

            case PatchStatus.Created:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.Created;
                break;

            case PatchStatus.Skipped:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.NotModified;
                return;

            case PatchStatus.Patched:
            case PatchStatus.NotModified:
                HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                break;

            default:
                throw new ArgumentOutOfRangeException();
        }

        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(command.PatchResult.Status));
            writer.WriteString(command.PatchResult.Status.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(command.PatchResult.ModifiedDocument));
            writer.WriteObject(command.PatchResult.ModifiedDocument);

            if (debugMode)
            {
                writer.WriteComma();
                writer.WritePropertyName(nameof(command.PatchResult.OriginalDocument));
                if (isTest)
                    writer.WriteObject(command.PatchResult.OriginalDocument);
                else
                    writer.WriteNull();

                writer.WriteComma();

                writer.WritePropertyName(nameof(command.PatchResult.Debug));

                context.Write(writer, new DynamicJsonValue
                {
                    ["Info"] = new DynamicJsonArray(command.DebugOutput),
                    ["Actions"] = command.DebugActions
                });
            }

            switch (command.PatchResult.Status)
            {
                case PatchStatus.Created:
                case PatchStatus.Patched:

                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.LastModified));
                    writer.WriteString(command.PatchResult.LastModified.GetDefaultRavenFormat());
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.ChangeVector));
                    writer.WriteString(command.PatchResult.ChangeVector);
                    writer.WriteComma();

                    writer.WritePropertyName(nameof(command.PatchResult.Collection));
                    writer.WriteString(command.PatchResult.Collection);
                    break;
            }

            writer.WriteEndObject();
        }
    }
}
