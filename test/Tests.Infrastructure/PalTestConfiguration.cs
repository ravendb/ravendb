using Raven.Server.Config;
using Raven.Server.ServerWide;
using Sparrow.Server.Platform;

namespace Tests.Infrastructure;

// the PAL reads PalConfiguration once, on the first native call, so every test base applies this before anything can open a Voron environment
public static class PalTestConfiguration
{
    static PalTestConfiguration()
    {
        var configuration = RavenConfiguration.CreateForTesting("Tests", ResourceType.Server, skipEnvironmentVariables: false);
        configuration.Initialize();

        PalConfiguration.LowPriorityIo = configuration.Storage.LowPriorityFlushAndSync;
        PalConfiguration.WriteMode = configuration.Storage.WriteMode;
        PalConfiguration.IoRingQueueSize = configuration.Storage.IoRingQueueSize;
    }

    public static void Apply()
    {
        // done by the static constructor, which runs once before the first call
    }
}
