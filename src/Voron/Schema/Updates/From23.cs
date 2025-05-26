using System;
using System.IO;
using Voron.Impl.FileHeaders;

namespace Voron.Schema.Updates;

public class From23  : IVoronSchemaUpdate
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

        Span<MetadataFile> metadata = stackalloc MetadataFile[1];
        headerAccessor.MetadataAccessor.FillMetadata(ref metadata[0]);

        versionAfterUpgrade = 24;
        return true;
    }
}
