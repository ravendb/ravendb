using System.IO;
using Voron.Impl.FileHeaders;

namespace Voron.Schema.Updates;

public class From23  : IVoronSchemaUpdate
{
    public bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
    {
        foreach (var unusedFile in Directory.GetFiles(options.JournalPath.FullPath, "recyclable-journal.*"))
        {
            File.Delete(unusedFile);
        }

        versionAfterUpgrade = 24;
        return true;
    }
}
