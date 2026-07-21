using System;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Client.Json.Serialization;
using Raven.Server.Documents.CdcSink.Test;
using Raven.Server.ServerWide.Context;
using Sparrow.Json;

namespace Raven.Server.Documents.CdcSink.Handlers.Processors;

internal sealed class CdcSinkHandlerProcessorForCdcTest : AbstractCdcSinkHandlerProcessorForTest<DatabaseRequestHandler, DocumentsOperationContext>
{
    public CdcSinkHandlerProcessorForCdcTest([NotNull] DatabaseRequestHandler requestHandler) : base(requestHandler)
    {
    }

    public override async ValueTask ExecuteAsync()
    {
        using (var cts = RequestHandler.CreateHttpRequestBoundTimeLimitedOperationToken(TimeSpan.FromMinutes(2)))
        using (ContextPool.AllocateOperationContext(out DocumentsOperationContext context))
        {
            var bodyJson = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "CdcTestRequest");
            var request = JsonDeserializationClient.CdcTestRequest(bodyJson);

            request.Configuration.Initialize(request.Connection);

            var result = await CdcSinkTestProcess.VerifyAsync(RequestHandler.Database, request.Configuration, cts.Token);

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream()))
            {
                context.Write(writer, result.ToJson());
            }
        }
    }
}
