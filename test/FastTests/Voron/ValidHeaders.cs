using System;
using System.Linq;
using System.Reflection;
using System.Runtime.InteropServices;
using FastTests.Voron.FixedSize;
using Voron.Data;
using Voron.Data.BTrees;
using Voron.Global;
using Voron.Impl.FileHeaders;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron
{
    public unsafe class ValidHeaders : NoDisposalNeeded
    {
        [Fact]
        public void ValidateNoOverlap()
        {
            var t = typeof(FileHeader);

            var pos = IntPtr.Zero;
            foreach (var fieldInfo in t.GetFields(BindingFlags.Instance | BindingFlags.Public)
                // GetFields has no guranteed sort order
                .OrderBy(x=> (long)Marshal.OffsetOf<FileHeader>(x.Name))
                )
            {
                var offsetOf = Marshal.OffsetOf<FileHeader>(fieldInfo.Name);
                if (pos != offsetOf)
                    Assert.False(true, fieldInfo.Name + " " + pos + " != " + offsetOf);
                pos += GetSizeOf(fieldInfo.FieldType);
            }
        }

        private int GetSizeOf(Type type)
        {
            var sizeOf = typeof(Marshal).GetMethods(BindingFlags.Public | BindingFlags.Static)
                .Single(x => x.Name == "SizeOf" && x.IsGenericMethodDefinition && x.GetParameters().Length == 0);
            return (int)sizeOf.MakeGenericMethod(type).Invoke(null, null);
        }

        [Fact]
        public void FileHeader()
        {
            var seed = 4;
            var rnd = new Random(seed);
            var ptr = stackalloc FileHeader[1];
            ptr->HeaderRevision = (long)rnd.NextDouble();

            ptr->Root.NumberOfEntries = (long)rnd.NextDouble();
            ptr->Root.BranchPages = (long)rnd.NextDouble();
            ptr->Root.Depth = rnd.Next();
            ptr->Root.Flags = TreeFlags.MultiValue;
            ptr->Root.LeafPages = (long)rnd.NextDouble();
            ptr->Root.OverflowPages = (long)rnd.NextDouble();
            ptr->Root.PageCount = (long)rnd.NextDouble();
            ptr->Root.RootObjectType = RootObjectType.VariableSizeTree;
            ptr->Root.RootPageNumber = (long)rnd.NextDouble();

            ptr->Journal.CurrentJournal = (long)rnd.NextDouble();
            ptr->Journal.LastSyncedJournal = (long)rnd.NextDouble();
            ptr->Journal.LastSyncedTransactionId = (long)rnd.NextDouble();

            ptr->IncrementalBackup.LastBackedUpJournal = (long)rnd.NextDouble();
            ptr->IncrementalBackup.LastBackedUpJournalPage = (long)rnd.NextDouble();
            ptr->IncrementalBackup.LastCreatedJournal = (long)rnd.NextDouble();

            ptr->LastPageNumber = (long)rnd.NextDouble();
            ptr->MagicMarker = Constants.MagicMarker;
            ptr->PageSize = Constants.Storage.PageSize;

            ptr->TransactionId = (long)rnd.NextDouble();
            ptr->Version = rnd.Next();


            rnd = new Random(seed);

            Assert.Equal(ptr->HeaderRevision, (long)rnd.NextDouble());

            Assert.Equal(ptr->Root.NumberOfEntries, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.BranchPages, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.Depth, rnd.Next());
            Assert.Equal(ptr->Root.Flags, TreeFlags.MultiValue);
            Assert.Equal(ptr->Root.LeafPages, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.OverflowPages, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.PageCount, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.RootObjectType, RootObjectType.VariableSizeTree);
            Assert.Equal(ptr->Root.RootPageNumber, (long)rnd.NextDouble());

            Assert.Equal(ptr->Journal.CurrentJournal, (long)rnd.NextDouble());
            Assert.Equal(ptr->Journal.LastSyncedJournal, (long)rnd.NextDouble());
            Assert.Equal(ptr->Journal.LastSyncedTransactionId, (long)rnd.NextDouble());

            Assert.Equal(ptr->IncrementalBackup.LastBackedUpJournal, (long)rnd.NextDouble());
            Assert.Equal(ptr->IncrementalBackup.LastBackedUpJournalPage, (long)rnd.NextDouble());
            Assert.Equal(ptr->IncrementalBackup.LastCreatedJournal, (long)rnd.NextDouble());

            Assert.Equal(ptr->LastPageNumber, (long)rnd.NextDouble());
            Assert.Equal(ptr->MagicMarker, Constants.MagicMarker);
            Assert.Equal(ptr->PageSize, Constants.Storage.PageSize);

            Assert.Equal(ptr->TransactionId, (long)rnd.NextDouble());
            Assert.Equal(ptr->Version, rnd.Next());


        }

        [Theory]
        [InlineDataWithRandomSeed]
        public void TransactionHeader(int seed)
        {
            var rnd = new Random(seed);
            var ptr = stackalloc TransactionHeader[1];

            ptr->LastPageNumber = (long)rnd.NextDouble();
            ptr->Root.NumberOfEntries = (long)rnd.NextDouble();
            ptr->Root.BranchPages = (long)rnd.NextDouble();
            ptr->Root.Depth = rnd.Next();
            ptr->Root.Flags = TreeFlags.MultiValue;
            ptr->Root.LeafPages = (long)rnd.NextDouble();
            ptr->Root.OverflowPages = (long)rnd.NextDouble();
            ptr->Root.PageCount = (long)rnd.NextDouble();
            ptr->Root.RootObjectType = RootObjectType.VariableSizeTree;
            ptr->Root.RootPageNumber = (long)rnd.NextDouble();
            ptr->TransactionId = (long)rnd.NextDouble();
            ptr->CompressedSize = rnd.Next();
            ptr->Hash = (ulong)rnd.Next();
            ptr->HeaderMarker = (ulong)rnd.NextDouble();
            ptr->NextPageNumber = (long)rnd.NextDouble();
            ptr->TxMarker = TransactionMarker.Commit;
            ptr->UncompressedSize = rnd.Next();
            ptr->PageCount = rnd.Next();

            rnd = new Random(seed);

            Assert.Equal(ptr->LastPageNumber, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.NumberOfEntries, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.BranchPages, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.Depth, rnd.Next());
            Assert.Equal(ptr->Root.Flags, TreeFlags.MultiValue);
            Assert.Equal(ptr->Root.LeafPages, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.OverflowPages, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.PageCount, (long)rnd.NextDouble());
            Assert.Equal(ptr->Root.RootObjectType, RootObjectType.VariableSizeTree);
            Assert.Equal(ptr->Root.RootPageNumber, (long)rnd.NextDouble());
            Assert.Equal(ptr->TransactionId, (long)rnd.NextDouble());
            Assert.Equal(ptr->CompressedSize, rnd.Next());
            Assert.Equal(ptr->Hash, (ulong)rnd.Next());
            Assert.Equal(ptr->HeaderMarker, (ulong)rnd.NextDouble());
            Assert.Equal(ptr->NextPageNumber, (long)rnd.NextDouble());
            Assert.Equal(ptr->TxMarker, TransactionMarker.Commit);
            Assert.Equal(ptr->UncompressedSize, rnd.Next());
            Assert.Equal(ptr->PageCount, rnd.Next());
        }
    }
}
