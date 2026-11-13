using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// OpenAI over the Responses API. The Chat Completions rule that forces reasoning_effort "none" for gpt-5.4+
// (OpenAiChatCompletionProvider) is not needed here: Responses accepts tools together with reasoning.
// Seed is not sent: Responses has no seed field.
internal sealed class OpenAiResponsesProvider : AbstractOpenAiResponsesProvider
{
    private readonly OpenAiSettings _settings;

    public OpenAiResponsesProvider(OpenAiSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    public override void AddHeaders(HttpRequestMessage request) => OpenAiShared.AddOpenAiHeaders(request, _settings);

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response) => OpenAiShared.ParseOpenAiError(content, response);
}
