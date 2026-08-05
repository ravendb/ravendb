using System;
using System.IO;
using Voron.Impl.FileHeaders;

namespace Voron.Schema.Updates;

public class From24  : IVoronSchemaUpdate
{
    public bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
    {
        foreach (var unusedFile in Directory.GetFiles(options.JournalPath.FullPath, "recyclable-journal.*"))
        {
            try
            {
                File.Delete(unusedFile);
            }
            catch
            {
                // it is safe to ignore this, since we are just trying 
                // to recover some disk space, and everything will still functions fine with this
            }
        }

        // The metadata file (and its JournalId) already exists at version 24. Regenerating the
        // id would make recovery skip every journal transaction stamped with the previous one.

        versionAfterUpgrade = 25;
        return true;
    }
}
