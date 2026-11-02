using System.IO;
using Voron.Impl.FileHeaders;

namespace Voron.Schema.Updates;

public sealed class From25 : IVoronSchemaUpdate
{
    public bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
    {
        // Version 26 is the 8.0 format. Compared to 7.2 (version 25):
        // - database.metadata holds the environment's JournalId, filled below
        // - transaction headers carry that JournalId, XORed with the incarnation of the journal file
        // - every journal starts with a journal header record holding its number and incarnation
        // - a root and its branches can share journals, registered with a linked journals record
        // - a transaction can carry the pages it freed and its durability watermark, and can be Zstd compressed
        // - headers.one / headers.two no longer track the current journal, the existence of the journal file does
        // - FixedSizeTree leaf pages can carry a tombstone bitmap

        // the recyclable journals left by 7.2 are deleted, 8.0 builds its own reuse pool
        foreach (var unusedFile in Directory.GetFiles(options.JournalPath.FullPath, "recyclable-journal.*"))
        {
            try
            {
                File.Delete(unusedFile);
            }
            catch
            {
                // best effort - if it stays, the 8.0 reuse pool can still take it: a reused journal gets a fresh
                // journal header record first, so the 7.2 transactions left in it are skipped as foreign
            }
        }

        headerAccessor.MetadataAccessor.Modify(headerAccessor.MetadataAccessor.FillMetadata);

        versionAfterUpgrade = 26;

        return true;
    }
}
