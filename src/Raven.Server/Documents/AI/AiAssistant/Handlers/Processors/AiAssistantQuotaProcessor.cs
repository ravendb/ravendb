using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Http;
using Raven.Server.Commercial;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;

internal class AiAssistantQuotaProcessor([NotNull] RequestHandler requestHandler) : AiAssistantHandlerProcessorBase(requestHandler)
{
    public override async ValueTask ExecuteAsync()
    {
        var requestMetadata = new AiAssistantRequestAuthentication();
        FulfillRequestMetadata(requestMetadata);

        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        using (var token = RequestHandler.CreateHttpRequestBoundOperationToken())
        {
            var response = await ApiHttpClient.PostAsync(
                    requestUri: "/api/v1/assistant/assist",
                    content: new StringContent(JsonConvert.SerializeObject(requestMetadata), Encoding.UTF8, "application/json"),
                    token: token.Token)
                .ConfigureAwait(false);

            if (response.IsSuccessStatusCode)
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                {
                    await writer.WriteStreamAsync(await response.Content.ReadAsStreamAsync(), token.Token);
                    await writer.FlushAsync(token.Token);
                }
            }

            var responseString = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);
            throw new InvalidOperationException(responseString);
        }
    }
}
