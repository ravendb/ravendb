// -----------------------------------------------------------------------
//  <copyright file="IncrementalBackupInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Runtime.InteropServices;

namespace Voron.Impl.Backup
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct IncrementalBackupInfo
    {
        [FieldOffset(0)]
        public long LastBackedUpJournal;

        [FieldOffset(8)] 
        public long LastBackedUpJournalPage;

        /// <summary>
        /// Up until version 8.0 - used to hold LastCreatedJournal, but we now
        /// use the existence of the journal file instead to mark this
        /// </summary>
        [FieldOffset(16)]
        public long Reserved;
    }
}
