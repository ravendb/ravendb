using Sparrow;
using Voron.Data.BTrees;
using Voron.Data.Fixed;

namespace Voron.Global
{
    public unsafe class Constants
    {
        public const ulong MagicMarker = 0xB16BAADC0DEF0015;

        public const ulong TransactionHeaderMarker = 0x1A4C92AD90ABC123;

        public static class Storage
        {
            public const int PageSize = 4 * Size.Kilobyte;
        }

        public static class Size
        {
            public const int Kilobyte = 1024;
            public const int Megabyte = 1024 * Kilobyte;
            public const int Gigabyte = 1024 * Megabyte;
            public const long Terabyte = 1024 * (long)Gigabyte;

            public const int Sector = 512;
        }


        /// <summary>
        /// If there are less than 2 keys in a page, we no longer have a tree
        /// This impacts the MaxKeySize available
        /// </summary>
        public const int MinKeysInPage = 2;

        public static readonly int FixedSizeTreePageHeaderSize = sizeof(FixedSizeTreePageHeader);

        public static readonly int TreePageHeaderSize = sizeof(TreePageHeader);

        public static readonly int NodeHeaderSize = sizeof(TreeNodeHeader);

        public const int PageNumberSize = sizeof(long);

        public const int NodeOffsetSize = sizeof(ushort);

        public const ushort SizeOfUInt = sizeof(uint);

        public const int CurrentVersion = 8;

        public const string RootTreeName = "$Root";
        public static readonly Slice RootTreeNameSlice;

        public const string MetadataTreeName = "$Database-Metadata";
        public static readonly Slice MetadataTreeNameSlice;

        public const string DatabaseFilename = "Raven.voron";
        public static readonly Slice DatabaseFilenameSlice;

        public const int DefaultMaxLogLengthBeforeCompaction = 64; //how much entries in log to keep before compacting it into snapshot

        static Constants()
        {
            Slice.From(StorageEnvironment.LabelsContext, RootTreeName, ByteStringType.Immutable, out RootTreeNameSlice);
            Slice.From(StorageEnvironment.LabelsContext, MetadataTreeName, ByteStringType.Immutable, out MetadataTreeNameSlice);
            Slice.From(StorageEnvironment.LabelsContext, DatabaseFilename, ByteStringType.Immutable, out DatabaseFilenameSlice);
        }
    }
}