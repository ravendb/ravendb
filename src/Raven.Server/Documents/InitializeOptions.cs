using System;

namespace Raven.Server.Documents
{
    [Flags]
    public enum InitializeOptions
    {
        None = 0,

        GenerateNewDatabaseId = 1,
        SkipLoadingDatabaseRecord = 2,
        /// <summary>
        /// This is required when we are restarting after a snapshot
        /// Since we use shared journals, and the index snapshot may be out of date, the
        /// current journal of the database may contain transactions of the _indexes_.
        /// So after a snapshot + shared journals - we have to create a new, empty,
        /// journal file to avoid linking to journals that have old transactions
        /// See test: Snapshot_should_have_correct_index_entries_after_snapshot_and_incremental_restore_counters
        /// </summary>
        ForceNewJournalFile = 4
    }
}
