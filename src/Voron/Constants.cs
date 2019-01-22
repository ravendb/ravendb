using System;
using Sparrow;
using Voron.Data.BTrees;
using Voron.Data.Compression;
using Voron.Data.Fixed;

namespace Voron.Global
{
    public unsafe class Constants
    {
        public const int CurrentVersion = 23;

        public const ulong MagicMarker = 0xB16BAADC0DEF0015;
        public const ulong TransactionHeaderMarker = 0x1A4C92AD90ABC123;

        public static class Size
        {
            public const int Kilobyte = 1024;
            public const int Megabyte = 1024 * Kilobyte;
            public const int Gigabyte = 1024 * Megabyte;
            public const long Terabyte = 1024 * (long)Gigabyte;

            public const int Sector = 512;
        }

        public static class Storage
        {
            public const int PageSize = 8 * Size.Kilobyte;

            static Storage()
            {
                GC.KeepAlive(new int[
                    // this is a way to have static assert
                    PageSize > ushort.MaxValue || PageSize < 4*Constants.Size.Kilobyte ||
                    PageSize% Size.Sector != 0
                        ? -1
                        : 0
                    ]);
            }
        }

        public static class Compression
        {
            public const int HeaderSize = CompressedNodesHeader.SizeOf;

            public const int MaxPageSize = 64 * Size.Kilobyte;

            static Compression()
            {
                Constants.Assert(() => HeaderSize == sizeof(CompressedNodesHeader), () => $"{nameof(CompressedNodesHeader)} size has changed and not updated at Voron.Global.Constants.");
            }
        }

        public static class FixedSizeTree
        {
            public const int PageHeaderSize = FixedSizeTreePageHeader.SizeOf;

            static FixedSizeTree()
            {
                Constants.Assert(() => PageHeaderSize == sizeof(FixedSizeTreePageHeader), () => $"{nameof(FixedSizeTreePageHeader)} size has changed and not updated at Voron.Global.Constants.");
            }
        }

        public static class Tree
        {
            public const int PageHeaderSize = TreePageHeader.SizeOf;
            public const int NodeHeaderSize = TreeNodeHeader.SizeOf;
            public const int NodeOffsetSize = sizeof(ushort);
            public const int PageNumberSize = sizeof(long);

            /// <summary>
            /// If there are less than 2 keys in a page, we no longer have a tree
            /// This impacts the MaxKeySize available
            /// </summary>
            public const int MinKeysInPage = 2;

            static Tree()
            {
                Constants.Assert(() => PageHeaderSize == sizeof(TreePageHeader), () => $"{nameof(TreePageHeader)} size has changed and not updated at Voron.Global.Constants.");
                Constants.Assert(() => NodeHeaderSize == sizeof(TreeNodeHeader), () => $"{nameof(TreeNodeHeader)} size has changed and not updated at Voron.Global.Constants.");
            }
        }

        public const string RootTreeName = "$Root";
        public static readonly Slice RootTreeNameSlice;

        public const string MetadataTreeName = "$Database-Metadata";
        public static readonly Slice MetadataTreeNameSlice;

        public const string DatabaseFilename = "Raven.voron";
        public static readonly Slice DatabaseFilenameSlice;
        public const string DatabaseRecordFileNameInBackup = "___database_record___";

        public const int DefaultMaxLogLengthBeforeCompaction = 64; //how much entries in log to keep before compacting it into snapshot       

        static Constants()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, RootTreeName, ByteStringType.Immutable, out RootTreeNameSlice);
                Slice.From(ctx, MetadataTreeName, ByteStringType.Immutable, out MetadataTreeNameSlice);
                Slice.From(ctx, DatabaseFilename, ByteStringType.Immutable, out DatabaseFilenameSlice);
            }
           }

        public static void Assert(Func<bool> condition, Func<string> reason)
        {
            if (!condition())
                throw new NotSupportedException($"Critical: A constant assertion has failed. Reason: {reason()}.");
        }
    }
}
