using System;

namespace Raven.Client.Documents.Operations.AI;

public interface IChatCompletionSettings
{
    public string ApiKey { get; }

    public string Model { get; }

    public string Endpoint { get; }

    public Uri GetBaseEndpointUri();
}
