using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;
using Sparrow.Json.Parsing;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;

internal class AiAssistantAssistProcessor([NotNull] RequestHandler requestHandler) : AiAssistantHandlerProcessorBase(requestHandler)
{
    public override async ValueTask ExecuteAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var requestBody = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "assist request");
            
            var modifications = new DynamicJsonValue(requestBody);
            requestBody.Modifications = modifications;
            FulfillRequestMetadata(modifications);
            
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            
            var response = await ApiHttpClient.PostAsync(
                    requestUri: "/api/v1/ai/assist",
                    content: new StringContent(context.ReadObject(requestBody, "ai-assist").ToString(), Encoding.UTF8, "application/json"),
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
