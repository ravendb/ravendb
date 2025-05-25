// -----------------------------------------------------------------------
//  <copyright file="JournalInfo.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Runtime.InteropServices;

namespace Voron.Impl.Journal
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public unsafe struct JournalInfo
    {
        public const int NumberOfReservedBytes = 3;

        /// <summary>
        /// Up until version 8.0 - this used to be CurrentJournal,
        /// this is no longer needed since we are using the existence
        /// of the actual journal file instead...
        /// </summary>
        [FieldOffset(0)]
        public long Reserved1;

        [FieldOffset(8)]
        public long LastSyncedJournal;

        [FieldOffset(16)]
        public long LastSyncedTransactionId;

        [FieldOffset(24)]
        public fixed byte Reserved2[NumberOfReservedBytes];

        [FieldOffset(27)]
        public JournalInfoFlags Flags;
    }

    [Flags]
    public enum JournalInfoFlags : byte
    {
        None = 0,
        IgnoreMissingLastSyncJournal = 1
    }
}
