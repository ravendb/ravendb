using System;
using System.Collections;
using Microsoft.Extensions.Configuration;

namespace Raven.Server.Monitoring.OpenTelemetry;

public class OpenTelemetryEnvironmentVariablesReader : ConfigurationProvider, IConfigurationSource
{
    private const string OTelPrefix = "OTEL_";

    public IConfigurationProvider Build(IConfigurationBuilder builder)
    {
        return this;
    }

    public override void Load()
    {
        var variables = Environment.GetEnvironmentVariables();
        foreach (DictionaryEntry variable in variables)
        {
            var key = variable.Key.ToString();

            if (string.IsNullOrEmpty(key) || key.StartsWith(OTelPrefix) == false)
                continue;

            Data[key] = variable.Value?.ToString();
        }
    }
}
