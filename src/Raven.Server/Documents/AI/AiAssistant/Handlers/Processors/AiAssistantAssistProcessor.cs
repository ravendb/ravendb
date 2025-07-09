using System;
using System.IO;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Client.Http;
using Raven.Server.Commercial;
using Raven.Server.Json;
using Raven.Server.ServerWide.Context;
using Raven.Server.Web;
using Sparrow;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.AiAssistant.Handlers.Processors;

internal class AiAssistantAssistProcessor([NotNull] RequestHandler requestHandler) : AiAssistantHandlerProcessorBase(requestHandler)
{
    public override async ValueTask ExecuteAsync()
    {
        using (ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
        {
            var requestBody = await context.ReadForMemoryAsync(RequestHandler.RequestBodyStream(), "assist request");
            var requestTypeFound = requestBody.TryGetMember(nameof(RequestType), out var requestTypeObject);
            PortableExceptions.ThrowIf<InvalidOperationException>(requestTypeFound == false || requestTypeObject is not LazyStringValue, $"AiAssistant requires '{nameof(RequestType)}' which doesn't exist.");

            if (requestTypeObject is not LazyStringValue requestTypeLsv || Enum.TryParse(requestTypeLsv, out RequestType requestType) == false)
                throw new InvalidOperationException($"Couldn't parse '{nameof(RequestType)}'.");
            
            var request = requestType switch
            {
                RequestType.RefineTextGenAi => GetRefineTextGenAiRequestBody(requestBody),
                _ => throw new NotImplementedException($"Request type '{requestType}' is not implemented.")
            };
            
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            
            var response = await ApiHttpClient.PostAsync(
                    requestUri: "/api/v1/assistant/assist",
                    content: new StringContent(request, Encoding.UTF8, "application/json"),
                    token: token.Token)
                .ConfigureAwait(false);


            if (response.IsSuccessStatusCode)
            {
                await using (var writer = new AsyncBlittableJsonTextWriter(context, RequestHandler.ResponseBodyStream(), token.Token))
                {
                    await writer.WriteStreamAsync(await response.Content.ReadAsStreamAsync(), token.Token);
                }
            }
            
            var responseString = await response.Content.ReadAsStringWithZstdSupportAsync().ConfigureAwait(false);
            throw new InvalidOperationException(responseString);
        }
    }
    
    
    private string GetRefineTextGenAiRequestBody(BlittableJsonReaderObject requestBody)
    {
        RefineTextGenAi request = JsonDeserializationServer.RefineTextGenAi(requestBody);
        FulfillRequestMetadata(request);
        
        return JsonConvert.SerializeObject(request);
    }
}
