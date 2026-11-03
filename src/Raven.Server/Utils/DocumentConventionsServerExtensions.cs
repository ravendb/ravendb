using System;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Sparrow;
using Sparrow.Platform;

namespace Raven.Client.Documents.Conventions;

internal static class DocumentConventionsServerExtensions
{
    private static readonly DocumentConventions ServerConventions = CreateServerConventions();

    extension(DocumentConventions)
    {
        public static DocumentConventions DefaultForServer => ServerConventions;
    }

    private static DocumentConventions CreateServerConventions()
    {
        var conventions = new DocumentConventions
        {
            Serialization = new NewtonsoftJsonSerializationConventions(),
            SendApplicationIdentifier = false,
            MaxContextSizeToKeep = new Size(PlatformDetails.Is32Bits == false ? 8 : 2, SizeUnit.Megabytes),
            HttpPooledConnectionLifetime = GetHttpPooledConnectionLifetime(),
            DisposeCertificate = false,
            DisableTopologyCache = true
        };

        var httpVersionAsString = Environment.GetEnvironmentVariable("RAVEN_HTTP_VERSION");
        if (httpVersionAsString != null)
        {
            try
            {
                conventions.HttpVersion = Version.Parse(httpVersionAsString);
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"Could not parse 'RAVEN_HTTP_VERSION' env variable with value '{httpVersionAsString}'.", e);
            }
        }

        conventions.Freeze();
        return conventions;
    }

    private static TimeSpan? GetHttpPooledConnectionLifetime()
    {
        var httpPooledConnectionLifetimeAsString = Environment.GetEnvironmentVariable("RAVEN_HTTP_POOLEDCONNECTIONLIFETIMEINSEC");
        if (httpPooledConnectionLifetimeAsString == null)
            return TimeSpan.FromMinutes(19);

        if (int.TryParse(httpPooledConnectionLifetimeAsString, out var httpPooledConnectionLifetime) == false)
            throw new InvalidOperationException($"Could not parse 'RAVEN_HTTP_POOLEDCONNECTIONLIFETIMEINSEC' env variable with value '{httpPooledConnectionLifetimeAsString}'.");

        return httpPooledConnectionLifetime < 0
            ? null
            : TimeSpan.FromSeconds(httpPooledConnectionLifetime);
    }
}
