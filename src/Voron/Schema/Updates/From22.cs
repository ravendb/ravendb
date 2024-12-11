using Sparrow;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;

namespace Voron.Schema.Updates
{
    public sealed class From22 : IVoronSchemaUpdate
    {
        public unsafe bool Update(int currentVersion, StorageEnvironmentOptions options, HeaderAccessor headerAccessor, out int versionAfterUpgrade)
        {
            headerAccessor.Modify((ref FileHeader header) => 
            {
                for (int i = 0; i < 3; i++)
                {
                    header.Journal.Reserved[i] = 0;
                }
                
                if (options.JournalExists(header.Journal.LastSyncedJournal))
                    header.Journal.Flags = JournalInfoFlags.None;
                else
                    header.Journal.Flags = JournalInfoFlags.IgnoreMissingLastSyncJournal;
            });

            versionAfterUpgrade = 23;

            return true;
        }
    }
}
