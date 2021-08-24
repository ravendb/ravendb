using System;
using System.Net;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Exceptions;
using Raven.Server.Documents.TransactionCommands;
using Raven.Server.Routing;
using Raven.Server.ServerWide.Context;
using Sparrow.Extensions;
using Sparrow.Json;

namespace Raven.Server.Documents.Handlers
{
    public class JsonPatchHandler : DatabaseRequestHandler
    {
        [RavenAction("/databases/*/json-patch", "PATCH", AuthorizationStatus.ValidUser, EndpointType.Write)]
        public async Task DocOperations()
        {
            var id = GetStringQueryString("id");

            using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
            {
                var blittable = await context.ReadForMemoryAsync(RequestBodyStream(), "json-patch");
                if (blittable == null)
                    throw new BadRequestException("Missing JSON content.");

                var commands = JsonPatchCommand.Parse(blittable);
                var jsonPatchCommand = new JsonPatchCommand(id, commands, returnDocument: true, context);
                await Database.TxMerger.Enqueue(jsonPatchCommand);

                switch (jsonPatchCommand._patchResult.Status)
                {
                    case PatchStatus.Patched:
                    case PatchStatus.NotModified:
                        HttpContext.Response.StatusCode = (int)HttpStatusCode.OK;
                        break;

                    default:
                        throw new ArgumentOutOfRangeException();
                }

                await using (var writer = new AsyncBlittableJsonTextWriter(context, ResponseBodyStream()))
                {
                    writer.WriteStartObject();

                    writer.WritePropertyName(nameof(jsonPatchCommand._patchResult.Status));
                    writer.WriteString(jsonPatchCommand._patchResult.Status.ToString());
                    
                    switch (jsonPatchCommand._patchResult.Status)
                    {
                        case PatchStatus.Patched:

                            writer.WriteComma();

                            writer.WritePropertyName(nameof(jsonPatchCommand._patchResult.ModifiedDocument));
                            writer.WriteObject(jsonPatchCommand._patchResult.ModifiedDocument);

                            writer.WriteComma();

                            writer.WritePropertyName(nameof(jsonPatchCommand._patchResult.LastModified));
                            writer.WriteString(jsonPatchCommand._patchResult.LastModified.GetDefaultRavenFormat());
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(jsonPatchCommand._patchResult.ChangeVector));
                            writer.WriteString(jsonPatchCommand._patchResult.ChangeVector);
                            writer.WriteComma();

                            writer.WritePropertyName(nameof(jsonPatchCommand._patchResult.Collection));
                            writer.WriteString(jsonPatchCommand._patchResult.Collection);
                            break;
                    }

                    writer.WriteEndObject();
                }
            }
        }
    }
}
