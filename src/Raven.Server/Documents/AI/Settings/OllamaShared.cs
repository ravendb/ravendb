using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// Ollama code that both wire APIs of the Ollama connector use: Chat Completions (OllamaChatCompletionProvider)
// and Responses (OllamaResponsesProvider). Code moves here when the Responses code needs it; it is never copied.
internal static class OllamaShared
{
    // Ollama serves its OpenAI-compatible endpoints under /v1
    public const string V1Prefix = "v1/";

    public static AiError ParseOllamaError(BlittableJsonReaderObject content, HttpResponseMessage response)
    {
        // Ollama does not provide structured error responses, so we return the raw content as the error message.
        return new AiError
        {
            ErrorType = ErrorType.Unknown,
            Message = content.ToString()
        };
    }
}
