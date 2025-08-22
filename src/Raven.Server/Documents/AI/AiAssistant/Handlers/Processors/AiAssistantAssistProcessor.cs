using System;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Newtonsoft.Json;
using Raven.Server.Commercial;
using Raven.Server.Documents.AI.AiAssistant.Requests;
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
            var operationTypeFound = requestBody.TryGetMember(nameof(AiAssistRequestBase.OperationType), out var operationTypeObject);
            PortableExceptions.ThrowIf<InvalidOperationException>(operationTypeFound == false || operationTypeObject is not LazyStringValue, $"AiAssistant request requires '{nameof(AiAssistRequestBase.OperationType)}', which wasn't provided.");

            if (operationTypeObject is not LazyStringValue operationTypeLsv || Enum.TryParse(operationTypeLsv, out AiAssistantOperationType operationType) == false)
                throw new InvalidOperationException($"Couldn't parse {operationTypeObject} as '{nameof(AiAssistantOperationType)}'.");
            
            var request = operationType switch
            {
                AiAssistantOperationType.RefineText => GetRefineTextGenAiRequestBody(requestBody),
                AiAssistantOperationType.RefineGenAiPrompt => GetRefineGenAiPromptRequestBody(requestBody),
                _ => throw new NotSupportedException($"Unsupported {nameof(AiAssistantOperationType)} - '{operationType}'")
            };
            
            using var token = RequestHandler.CreateHttpRequestBoundOperationToken();
            
            var response = await ApiHttpClient.PostAsync(
                    requestUri: "/api/v1/ai/assist",
                    content: new StringContent(request, Encoding.UTF8, "application/json"),
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
    
    private string GetRefineTextGenAiRequestBody(BlittableJsonReaderObject requestBody)
    {
        RefineTextRequest request = JsonDeserializationServer.RefineTextRequest(requestBody);
        FulfillRequestMetadata(request);
        
        return JsonConvert.SerializeObject(request);
    }

    private string GetRefineGenAiPromptRequestBody(BlittableJsonReaderObject requestBody)
    {
        RefineGenAiPromptRequest request = JsonDeserializationServer.RefineGenAiPromptRequest(requestBody);
        FulfillRequestMetadata(request);
        
        return JsonConvert.SerializeObject(request);
    }
}
