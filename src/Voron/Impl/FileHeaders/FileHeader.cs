﻿using System;
 using System.Runtime.InteropServices;
using Voron.Data.BTrees;
using Voron.Impl.Backup;
using Voron.Impl.Journal;

namespace Voron.Impl.FileHeaders
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct FileHeader
    {
        /// <summary>
        /// This is _always_ the last 8 bytes
        /// </summary>
        public static int HashOffset = sizeof(FileHeader) - sizeof(ulong);
        
        public static int TransactionIdOffset = (int)Marshal.OffsetOf<FileHeader>(nameof(TransactionId));

        /// <summary>
        /// Just a value chosen to mark our files headers, this is used to 
        /// make sure that we are opening the right format file
        /// </summary>
        [FieldOffset(0)]
        public ulong MagicMarker;

        /// <summary>
        /// The version of the data, used for versioning / conflicts
        /// </summary>
        [FieldOffset(8)]
        public int Version;

        /// <summary>
        /// Incremented on every header modification
        /// </summary>
        [FieldOffset(12)]
        public long HeaderRevision;

        /// <summary>
        /// The transaction id that committed this page
        /// </summary>
        [FieldOffset(20)]
        public long TransactionId;

        /// <summary>
        /// The last used page number for this file
        /// </summary>
        [FieldOffset(28)]
        public long LastPageNumber;

        /// <summary>
        /// The root node for the main tree
        /// </summary>
        [FieldOffset(36)]
        public TreeRootHeader Root;

        /// <summary>
        /// Information about the journal log info
        /// </summary>
        [FieldOffset(98)] 
        public JournalInfo Journal;

        /// <summary>
        /// Information about an incremental backup
        /// </summary>
        [FieldOffset(126)] 
        public IncrementalBackupInfo IncrementalBackup;

        /// <summary>
        /// The page size for the data file
        /// </summary>
        [FieldOffset(150)]
        public int PageSize;

        /// <summary>
        /// The database id for this file
        /// </summary>
        [FieldOffset(154)]
        public Guid DatabaseId;
        
        /// <summary>
        /// Hash of the header used for validation
        /// </summary>
        [FieldOffset(170)]
        public ulong Hash;
 
        public override string ToString()
        {
            return
                $"{nameof(Version)}: {Version}, {nameof(HeaderRevision)}: {HeaderRevision}, {nameof(TransactionId)}: {TransactionId}, {nameof(LastPageNumber)}: {LastPageNumber}, " +
                $"{nameof(Root.RootPageNumber)}: {Root.RootPageNumber}, " +
                $"{nameof(Journal.CurrentJournal)}: {Journal.CurrentJournal}, {nameof(DatabaseId)}: {DatabaseId},  {nameof(Journal.LastSyncedJournal)}: {Journal.LastSyncedJournal},  {nameof(Journal.LastSyncedTransactionId)}: {Journal.LastSyncedJournal}, {nameof(Journal.Flags)}: {Journal.Flags}";
        }
    }
}
