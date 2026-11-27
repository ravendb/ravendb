using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

internal class AzureOpenAiChatCompletionProvider : AbstractOpenAiChatCompletionProvider
{
    private new readonly AzureOpenAiSettings _settings;

    public AzureOpenAiChatCompletionProvider(AzureOpenAiSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    public override string GetRelativeCompletionUri() => $"openai/deployments/{_settings.DeploymentName}/chat/completions?api-version={AzureOpenAiShared.ChatCompletionsApiVersion}";

    public override string GetRelativeModelsUri() => AzureOpenAiShared.ModelsUri;
    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        return AzureOpenAiShared.ParseAzureError(content, response);
    }

    public override string GetRefusal(BlittableJsonReaderObject choice0, BlittableJsonReaderObject message, bool streaming, out bool isCompleteMessage)
    {
        // The messages below are full sentences derived from the content-filter annotations - a streaming
        // caller must keep one, not concatenate the same block across chunks.
        isCompleteMessage = true;

        // Azure annotates the filtered categories/severities in content_filter_results - prefer that detail.
        if (choice0.TryGet(AzureOpenAiShared.FiltersConstants.ContentFilterResults, out BlittableJsonReaderObject filtersObj)
            && filtersObj != null
            && AzureOpenAiShared.GetFiltersMessage(filtersObj, out var refusal))
            return refusal;

        // Otherwise fall back to the OpenAI default (explicit refusal field, or finish_reason == "content_filter").
        return base.GetRefusal(choice0, message, streaming, out isCompleteMessage);
    }
}
