using Voron.Impl.FileHeaders;

namespace Voron.Schema.Updates;

/// <summary>
/// Fixed size tree leaf pages now keep a tombstone bitmap at the end of the page, and pages written with it
/// are unreadable by older versions - they would hand out deleted entries as if they were live. The pages
/// that are already in the file are readable as they are and are converted lazily as writes touch them, so
/// there is nothing to migrate here, the version bump exists to keep older binaries from opening the file.
/// </summary>
public class From25 : IVoronSchemaUpdate
{
    public bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
    {
        versionAfterUpgrade = 26;
        return true;
    }
}
