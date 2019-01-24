using Sparrow;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;

namespace Voron.Schema.Updates
{
    public class From22 : IVoronSchemaUpdate
    {
        public unsafe bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
        {
            headerAccessor.Modify(header =>
            {
                Memory.Set(header->Journal.Reserved, 0, 3);
                
                if (options.JournalExists(header->Journal.LastSyncedJournal))
                    header->Journal.Flags = JournalInfoFlags.None;
                else
                    header->Journal.Flags = JournalInfoFlags.IgnoreMissingLastSyncJournal;
            });

            versionAfterUpgrade = 23;

            return true;
        }
    }
}
