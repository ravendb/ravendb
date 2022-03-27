using Raven.Client.Documents.Operations.Configuration;
using Raven.Server.ServerWide;

namespace Raven.Server.Utils.Configuration;

public static class ClientConfigurationHelper
{
    public static bool HasClientConfigurationChanged(ClientConfiguration clientConfiguration, ServerStore serverStore, long index)
    {
        var serverIndex = GetClientConfigurationEtag(clientConfiguration, serverStore);
        return index < serverIndex;
    }

    public static long GetClientConfigurationEtag(ClientConfiguration clientConfiguration, ServerStore serverStore)
    {
        return clientConfiguration == null || clientConfiguration.Disabled && serverStore.LastClientConfigurationIndex > clientConfiguration.Etag
            ? serverStore.LastClientConfigurationIndex
            : clientConfiguration.Etag;
    }
}
