using Raven.Client.Documents.Operations.AI;

namespace Raven.Quill.Contracts;

public sealed record AiConnectionStringListItemResponse(
    string Name,
    string Identifier,
    AiModelType ModelType,
    AiConnectorType Provider);
