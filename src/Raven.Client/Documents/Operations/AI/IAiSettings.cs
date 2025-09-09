using System;

namespace Raven.Client.Documents.Operations.AI;

internal interface IAiSettings
{
    public string ApiKey { get; }

    public string Model { get; }

    public string Endpoint { get; }

    public Uri GetBaseEndpointUri();
}
