using Voron.Impl.FileHeaders;

namespace Voron.Schema.Updates;

public sealed class From24 : IVoronSchemaUpdate
{
    public bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
    {
        // Version 25 keeps a counter of collapsed levels in the spare page flag bits (PageCollapsedLevels, RavenDB-27533).
        // No data migration is needed - pages written by v24 have those bits cleared.

        versionAfterUpgrade = 25;

        return true;
    }
}
