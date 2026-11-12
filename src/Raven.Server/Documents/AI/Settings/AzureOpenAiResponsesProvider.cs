using System.Net.Http;
using Raven.Client.Documents.Operations.AI;
using Sparrow.Json;

namespace Raven.Server.Documents.AI.Settings;

// Azure OpenAI over the Responses API, through the GA v1 path (no api-version). Azure resolves the deployment from the
// body's "model", so that carries the deployment name. Seed is not sent: Azure has none and Responses has no seed field.
internal sealed class AzureOpenAiResponsesProvider : AbstractOpenAiResponsesProvider
{
    private readonly AzureOpenAiSettings _settings;

    public AzureOpenAiResponsesProvider(AzureOpenAiSettings settings)
        : base(settings)
    {
        _settings = settings;
    }

    public override string GetRelativeCompletionUri() => "openai/v1/responses";

    public override string GetRelativeModelsUri() => AzureOpenAiShared.ModelsUri;

    protected override string RequestModel => _settings.DeploymentName;

    public override AiError ParseError(BlittableJsonReaderObject content, HttpResponseMessage response) => AzureOpenAiShared.ParseAzureError(content, response);

    protected override string GetProviderRefusal(BlittableJsonReaderObject response) => AzureOpenAiShared.GetBlockedContentFilterMessage(response);
}
