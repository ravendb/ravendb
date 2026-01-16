using System.Diagnostics.CodeAnalysis;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;

internal class AiAssistantCheckConsentProcessor([NotNull] RequestHandler requestHandler) : AiAssistantHandlerProcessorBase(requestHandler)
{
    public override async ValueTask ExecuteAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            var request = new DynamicJsonValue();
            FulfillRequestMetadata(request);
            
            var response = await ApiHttpClient.PostAsync(
                    relativeUri: "/api/v1/ai/check-consent",
                    content: new StringContent(context.ReadObject(request,"check-consent").ToString(), Encoding.UTF8, "application/json"),
                    token: token.Token)
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
                HttpContext.Response.StatusCode = (int)response.StatusCode;

            await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
            {
                await writer.WriteStreamAsync(await response.Content.ReadAsStreamAsync(), token.Token);
                await writer.FlushAsync(token.Token);
            }
        }
    }
}
