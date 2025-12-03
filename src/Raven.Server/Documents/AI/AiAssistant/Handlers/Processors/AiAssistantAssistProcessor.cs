using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
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
            var content = new StringContent(context.ReadObject(requestBody, "ai-assist").ToString(), Encoding.UTF8, "application/json");
            using var request = new HttpRequestMessage
            {
                Method = HttpMethod.Post,
                Content = content,
                RequestUri = new Uri("/api/v1/ai/assist", UriKind.Relative)
            };

            using var response = await ApiHttpClient.SendAsync(request, token.Token).ConfigureAwait(false);

            if (response.IsSuccessStatusCode == false)
                HttpContext.Response.StatusCode = (int)response.StatusCode;

            var contentType = response.Content.Headers.ContentType.ToString();
            HttpContext.Response.Headers.ContentType = contentType;

            if (response.IsSuccessStatusCode && contentType == "text/event-stream")
                RequestHandler.DisableResponseBuffering();

            await response.Content.CopyToAsync(RequestHandler.ResponseBodyStream(), token.Token);
        }
    }
}
