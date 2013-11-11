using System.Collections;
using System.Collections.Generic;
using System.IO;
using Voron.Trees;
using Voron.Util.Conversion;

namespace Voron.Impl.FreeSpace
{
    public class FreeSpaceHandling : IFreeSpaceHandling
    {
        private const int NumberOfPagesInSection = 256 * 8; // 256 bytes, 8 bits per byte = 2,048 - each section 8 MB in size
        private readonly StorageEnvironment _env;
        private readonly Slice _freePagesCount;

        public FreeSpaceHandling(StorageEnvironment env)
        {
            _freePagesCount = new Slice(EndianBitConverter.Big.GetBytes(long.MinValue));
            _env = env;
        }

        public long? TryAllocateFromFreeSpace(Transaction tx, int num)
        {
            if (tx.State.FreeSpaceRoot == null)
                return null; // initial setup

            using (var it = tx.State.FreeSpaceRoot.Iterate(tx))
            {
                var buffer = new byte[8];
                var key = new Slice(buffer);

                if (it.Seek(key) == false)
                    return null;

                if (num < NumberOfPagesInSection)
                {
                    return TryFindSmallValue(tx, it, num);
                }
                return TryFindLargeValue(it, num);
            }
        }

        private long? TryFindLargeValue(TreeIterator it, int num)
        {
            // TODO: Daniel's have fun with it
            return null;
        }

        private long? TryFindSmallValue(Transaction tx, TreeIterator it, int num)
        {
            do
            {
                using (var stream = it.CreateStreamForCurrent())
                {
                    var current = new StreamBitArray(stream);
                    var currentSectionId = it.CurrentKey.ToInt64();

                        long? page;
                    if (current.SetCount < num)
                    {
                        if (TryFindSmallValueMergingTwoSections(tx, it, num, current, currentSectionId, out page))
                            return page;
                        continue;
                    }

                    if (TryFindContinuousRange(tx, it, num, current,currentSectionId, out page))
                        return page;

                    //could not find a continuous so trying to merge
                    if (TryFindSmallValueMergingTwoSections(tx, it, num, current, currentSectionId, out page))
                        return page;
                }
            } while (it.MoveNext());

            return null;
        }

        private bool TryFindContinuousRange(Transaction tx, TreeIterator it, int num, StreamBitArray current,long currentSectionId, out long? page)
        {
            page = -1;
            var start = -1;
            var count = 0;
            for (int i = 0; i < NumberOfPagesInSection; i++)
            {
                if (current.Get(i))
                {
                    if (start == -1)
                        start = i;
                    count++;
                    if (count == num)
                    {
                        page = currentSectionId * NumberOfPagesInSection  + start ;
                        break;
                    }
                }
                else
                {
                    start = -1;
                    count = 0;
                }
            }

            if(count != num)
                return false;

            if (current.SetCount == num)
            {
                tx.State.FreeSpaceRoot.Delete(tx, it.CurrentKey);
            }
            else
            {
                for (int i = 0; i < num; i++)
                {
                    current.Set(i + start, false);
                }

                tx.State.FreeSpaceRoot.Add(tx, it.CurrentKey, current.ToStream());
            }

            return true;
        }

        private static bool TryFindSmallValueMergingTwoSections(Transaction tx, TreeIterator it, int num, StreamBitArray current, long currentSectionId, out long? result)
        {
            result = -1;
            var currentRange = current.GetEndRangeCount();
            if (currentRange == 0)
                return false;

            var nextSectionId = currentSectionId + 1;

            var nextId = new Slice(EndianBitConverter.Big.GetBytes(nextSectionId));
            var read = tx.State.FreeSpaceRoot.Read(tx, nextId);
            if (read == null)
                return false;

            var next = new StreamBitArray(read.Stream);

            var nextRange = num - currentRange;
            if (next.HasStartRangeCount(nextRange) == false)
                return false;

            if (next.SetCount == nextRange)
            {
                tx.State.FreeSpaceRoot.Delete(tx, nextId);
            }
            else
            {
                for (int i = 0; i < nextRange; i++)
                {
                    next.Set(i, false);
                }
                tx.State.FreeSpaceRoot.Add(tx, nextId, next.ToStream());
            }

            if (current.SetCount == currentRange)
            {
                tx.State.FreeSpaceRoot.Delete(tx, it.CurrentKey);
            }
            else
            {
                for (int i = 0; i < currentRange; i++)
                {
                    current.Set(NumberOfPagesInSection - 1 - i, false);
                }
                tx.State.FreeSpaceRoot.Add(tx, nextId, next.ToStream());
            }


            result = currentSectionId * NumberOfPagesInSection + currentRange;
            return true;
        }

        public long GetFreePageCount()
        {
            using (var tx = _env.NewTransaction(TransactionFlags.Read))
            {
                var readResult = tx.State.FreeSpaceRoot.Read(tx, _freePagesCount);
                if (readResult == null)
                    return 0;
                using (var br = new BinaryReader(readResult.Stream))
                {
                    return br.ReadInt64();
                }
            }
        }

        public List<long> AllPages(Transaction tx)
        {
            return tx.State.FreeSpaceRoot.AllPages(tx);
        }

        public void FreePage(Transaction tx, long pageNumber)
        {
            var section = pageNumber / NumberOfPagesInSection;
            var sectionKey = new Slice(EndianBitConverter.Big.GetBytes(section));
            var result = tx.State.FreeSpaceRoot.Read(tx, sectionKey);
            var sba = result == null ? new StreamBitArray() : new StreamBitArray(result.Stream);
            sba.Set((int)(pageNumber % NumberOfPagesInSection), true);
            tx.State.FreeSpaceRoot.Add(tx, sectionKey, sba.ToStream());
        }
    }
}