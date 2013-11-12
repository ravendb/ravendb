using System.Runtime.InteropServices;
using Voron.Impl.Backup;
using Voron.Impl.Journal;

namespace Voron.Impl.FileHeaders
{
    [StructLayout(LayoutKind.Explicit, Pack = 1)]
    public struct FileHeader
    {
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
        /// The transaction id that committed this page
        /// </summary>
        [FieldOffset(12)]
        public long TransactionId;

        /// <summary>
        /// The last used page number for this file
        /// </summary>
        [FieldOffset(20)]
        public long LastPageNumber;

        /// <summary>
        /// The root node for free space
        /// </summary>
        [FieldOffset(28)] 
        public TreeRootHeader FreeSpace;

        /// <summary>
        /// The root node for the main tree
        /// </summary>
        [FieldOffset(90)]
        public TreeRootHeader Root;

        /// <summary>
        /// Information about the journal log info
        /// </summary>
        [FieldOffset(152)] 
        public JournalInfo Journal;

		/// <summary>
		/// Information about an incremental backup
		/// </summary>
	    [FieldOffset(192)] 
		public IncrementalBackupInfo IncrementalBackup;
    }
}