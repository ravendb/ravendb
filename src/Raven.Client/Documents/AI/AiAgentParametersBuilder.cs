using System;
using System.Collections.Generic;

namespace Raven.Client.Documents.AI;

public interface IAiAgentParametersBuilder
{
    IAiAgentParametersBuilder AddParameter(string key, string value);

    internal Dictionary<string, object> GetParameters();
}

internal class AiAgentParametersBuilder : IAiAgentParametersBuilder
{
    private readonly Dictionary<string, object> _parameters = new(StringComparer.OrdinalIgnoreCase);

    public IAiAgentParametersBuilder AddParameter(string key, string value)
    {
        _parameters[key] = value;
        return this;
    }

    public Dictionary<string, object> GetParameters() => _parameters.Count == 0 ? null : _parameters;
}
