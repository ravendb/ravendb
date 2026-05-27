using Raven.Client.Documents.Operations.AI;

namespace Raven.AiAppliance.Contracts;

public sealed record AiConnectionStringListItemResponse(
    string Name,
    string Identifier,
    AiModelType ModelType,
    AiConnectorType Provider);
