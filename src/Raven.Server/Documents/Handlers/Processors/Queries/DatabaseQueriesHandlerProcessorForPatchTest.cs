using System;
using System.Net;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Documents.Operations;
using Raven.Server.Documents.Patch;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using PatchRequest = Raven.Server.Documents.Patch.PatchRequest;

namespace Raven.Server.Documents.Handlers.Processors.Queries;

internal class DatabaseQueriesHandlerProcessorForPatchTest : AbstractQueriesHandlerProcessorForPatchTest<QueriesHandler, DocumentsOperationContext>
{

    public DatabaseQueriesHandlerProcessorForPatchTest([NotNull] QueriesHandler requestHandler) : base(requestHandler, requestHandler.Database.QueryMetadataCache)
    {
    }

    protected override async ValueTask HandleDocumentPatchTestAsync(IndexQueryServerSide query, string docId, DocumentsOperationContext context)
    {
        var patch = new PatchRequest(query.Metadata.GetUpdateBody(query.QueryParameters), PatchRequestType.Patch, query.Metadata.DeclaredFunctions);

        var command = new PatchDocumentCommand(context, docId,
            expectedChangeVector: null,
            skipPatchIfChangeVectorMismatch: false,
            patch: (patch, query.QueryParameters),
            patchIfMissing: (null, null),
            identityPartsSeparator: context.DocumentDatabase.IdentityPartsSeparator,
            createIfMissing: null,
            debugMode: true,
            isTest: true,
            collectResultsNeeded: true,
            returnDocument: false
        );

        using (context.OpenWriteTransaction())
        {
            command.Execute(context, null);
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

        await WritePatchResultToResponseAsync(context, command);
    }

    private async ValueTask WritePatchResultToResponseAsync(DocumentsOperationContext context, PatchDocumentCommand command)
    {
        await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
        {
            writer.WriteStartObject();

            writer.WritePropertyName(nameof(command.PatchResult.Status));
            writer.WriteString(command.PatchResult.Status.ToString());
            writer.WriteComma();

            writer.WritePropertyName(nameof(command.PatchResult.ModifiedDocument));
            writer.WriteObject(command.PatchResult.ModifiedDocument);

            writer.WriteComma();
            writer.WritePropertyName(nameof(command.PatchResult.OriginalDocument));
            writer.WriteObject(command.PatchResult.OriginalDocument);

            writer.WriteComma();

            writer.WritePropertyName(nameof(command.PatchResult.Debug));

            context.Write(writer, new DynamicJsonValue
            {
                ["Output"] = new DynamicJsonArray(command.DebugOutput),
                ["Actions"] = command.DebugActions
            });

            writer.WriteEndObject();
        }
    }
}
