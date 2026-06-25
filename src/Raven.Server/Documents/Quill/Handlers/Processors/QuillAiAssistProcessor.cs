using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.Quill.Handlers.Processors;

internal sealed class QuillAiAssistProcessor([NotNull] RequestHandler requestHandler) : QuillProxyProcessorBase(requestHandler)
{
    public override async ValueTask ExecuteAsync()
    {
        if (TryAuthorize() == false)
            return;

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var requestBody = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "quill-assist request");

            var modifications = new DynamicJsonValue(requestBody);
            requestBody.Modifications = modifications;
            FulfillRequestMetadata(modifications);

            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            using var content = new StringContent(context.ReadObject(requestBody, "quill-ai-assist").ToString(), Encoding.UTF8, "application/json");

            await ProxyAsync("/api/v1/ai/assist", content, token.Token).ConfigureAwait(false);
        }
    }
}
