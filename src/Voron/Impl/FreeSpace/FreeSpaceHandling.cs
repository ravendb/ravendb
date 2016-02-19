using System.Collections.Generic;
using Voron.Data.Fixed;

namespace Voron.Impl.FreeSpace
{
    public class FreeSpaceHandling : IFreeSpaceHandling
    {
        private FreeSpaceHandlingDisabler DisableStatus = new FreeSpaceHandlingDisabler();
        internal const int NumberOfPagesInSection = 2048;

        public static bool IsFreeSpaceTreeName(string name)
        {
            return name == "$free-space";
        }

        private readonly static Slice FreeSpaceKey = "$free-space";

        public long? TryAllocateFromFreeSpace(LowLevelTransaction tx, int num)
        {
            if (tx.RootObjects == null)
                return null;

            if (DisableStatus.DisableCount > 0)
                return null;

            var freeSpaceTree = GetFreeSpaceTree(tx);


            if (freeSpaceTree.NumberOfEntries == 0)
                return null;

            using (var it = freeSpaceTree.Iterate())
            {
                if (it.Seek(0) == false)
                    return null;

                if (num < NumberOfPagesInSection)
                {
                    return TryFindSmallValue(freeSpaceTree, it, num);
                }
                return TryFindLargeValue(freeSpaceTree, it, num);
            }
        }

        private long? TryFindLargeValue(FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it, int num)
        {
            int numberOfNeededFullSections = num / NumberOfPagesInSection;
            int numberOfExtraBitsNeeded = num % NumberOfPagesInSection;

            var info = new FoundSectionsInfo();
            do
            {
                var stream = it.CreateReaderForCurrent();
                {
                    var current = new StreamBitArray(stream);
                    var currentSectionId = it.CurrentKey;

                    //need to find full free pages
                    if (current.SetCount < NumberOfPagesInSection)
                    {
                        info.Clear();
                        continue;
                    }

                    //those sections are not following each other in the memory
                    if (info.StartSectionId != null && currentSectionId != info.StartSectionId + info.Sections.Count)
                    {
                        info.Clear();
                    }

                    //set the first section of the sequence
                    if (info.StartSection == -1)
                    {
                        info.StartSection = it.CurrentKey;
                        info.StartSectionId = currentSectionId;
                    }

                    info.Sections.Add(it.CurrentKey);

                    if (info.Sections.Count != numberOfNeededFullSections)
                        continue;

                    //we found enough full sections now we need just a bit more
                    if (numberOfExtraBitsNeeded == 0)
                    {
                        foreach (var section in info.Sections)
                        {
                            freeSpaceTree.Delete(section);
                        }

                        return info.StartSectionId * NumberOfPagesInSection;
                    }

                    var nextSectionId = currentSectionId + 1;
                    var read = freeSpaceTree.Read(nextSectionId);
                    if (read == null)
                    {
                        //not a following next section
                        info.Clear();
                        continue;
                    }

                    var next = new StreamBitArray(read.CreateReader());

                    if (next.HasStartRangeCount(numberOfExtraBitsNeeded) == false)
                    {
                        //not enough start range count
                        info.Clear();
                        continue;
                    }

                    //mark selected bits to false
                    if (next.SetCount == numberOfExtraBitsNeeded)
                    {
                        freeSpaceTree.Delete(nextSectionId);
                    }
                    else
                    {
                        for (int i = 0; i < numberOfExtraBitsNeeded; i++)
                        {
                            next.Set(i, false);
                        }
                        freeSpaceTree.Add(nextSectionId, next.ToSlice());
                    }

                    foreach (var section in info.Sections)
                    {
                        freeSpaceTree.Delete(section);
                    }

                    return info.StartSectionId * NumberOfPagesInSection;
                }
            } while (it.MoveNext());

            return null;
        }

        private class FoundSectionsInfo
        {

            public List<long> Sections = new List<long>();

            public long StartSection = -1;

            public long? StartSectionId;


            public void Clear()
            {
                StartSection = -1;
                StartSectionId = null;
                Sections.Clear();
            }
        }


        private long? TryFindSmallValue(FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it, int num)
        {
            do
            {
                var current = new StreamBitArray(it.CreateReaderForCurrent());

                long? page;
                if (current.SetCount < num)
                {
                    if (TryFindSmallValueMergingTwoSections(freeSpaceTree, it.CurrentKey, num, current, out page))
                        return page;
                    continue;
                }

                if (TryFindContinuousRange(freeSpaceTree, it, num, current, it.CurrentKey, out page))
                    return page;

                //could not find a continuous so trying to merge
                if (TryFindSmallValueMergingTwoSections(freeSpaceTree, it.CurrentKey, num, current, out page))
                    return page;
            } while (it.MoveNext());

            return null;
        }

        private bool TryFindContinuousRange(FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it, int num, 
            StreamBitArray current, long currentSectionId, out long? page)
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
                        page = currentSectionId * NumberOfPagesInSection + start;
                        break;
                    }
                }
                else
                {
                    start = -1;
                    count = 0;
                }
            }

            if (count != num)
                return false;

            if (current.SetCount == num)
            {
                freeSpaceTree.Delete(it.CurrentKey);
            }
            else
            {
                for (int i = 0; i < num; i++)
                {
                    current.Set(i + start, false);
                }

                freeSpaceTree.Add(it.CurrentKey, current.ToSlice());
            }

            return true;
        }

        private static bool TryFindSmallValueMergingTwoSections(FixedSizeTree freeSpacetree, long currentSectionId, int num, 
            StreamBitArray current, out long? result)
        {
            result = -1;
            var currentEndRange = current.GetEndRangeCount();
            if (currentEndRange == 0)
                return false;

            var nextSectionId = currentSectionId + 1;

            var read = freeSpacetree.Read(nextSectionId);
            if (read == null)
                return false;

            var next = new StreamBitArray(read.CreateReader());

            var nextRange = num - currentEndRange;
            if (next.HasStartRangeCount(nextRange) == false)
                return false;

            if (next.SetCount == nextRange)
            {
                freeSpacetree.Delete(nextSectionId);
            }
            else
            {
                for (int i = 0; i < nextRange; i++)
                {
                    next.Set(i, false);
                }
                freeSpacetree.Add(nextSectionId, next.ToSlice());
            }

            if (current.SetCount == currentEndRange)
            {
                freeSpacetree.Delete(currentSectionId);
            }
            else
            {
                for (int i = 0; i < currentEndRange; i++)
                {
                    current.Set(NumberOfPagesInSection - 1 - i, false);
                }
                freeSpacetree.Add(currentSectionId, current.ToSlice());
            }


            result = currentSectionId * NumberOfPagesInSection + (NumberOfPagesInSection - currentEndRange);
            return true;
        }

        public List<long> AllPages(LowLevelTransaction tx)
        {
            var freeSpaceTree = GetFreeSpaceTree(tx);

            if (freeSpaceTree.NumberOfEntries == 0)
                return new List<long>();

            using (var it = freeSpaceTree.Iterate())
            {
                if (it.Seek(0) == false)
                    return new List<long>();

                var freePages = new List<long>();

                do
                {
                    var stream = it.CreateReaderForCurrent();

                    var current = new StreamBitArray(stream);
                    var currentSectionId = it.CurrentKey;

                    for (var i = 0; i < NumberOfPagesInSection; i++)
                    {
                        if (current.Get(i))
                            freePages.Add(currentSectionId * NumberOfPagesInSection + i);
                    }
                } while (it.MoveNext());

                return freePages;
            }
        }

        public void FreePage(LowLevelTransaction tx, long pageNumber)
        {
            var freeSpaceTree = GetFreeSpaceTree(tx);

            var section = pageNumber / NumberOfPagesInSection;
            var result = freeSpaceTree.Read(section);
            var sba = result == null ? new StreamBitArray() : new StreamBitArray(result.CreateReader());
            sba.Set((int)(pageNumber % NumberOfPagesInSection), true);
            freeSpaceTree.Add(section, sba.ToSlice());
        }

        public long GetFreePagesOverhead(LowLevelTransaction tx)
        {
            return GetFreeSpaceTree(tx).PageCount;
        }

        public IEnumerable<long> GetFreePagesOverheadPages(LowLevelTransaction tx)
        {
            return GetFreeSpaceTree(tx).AllPages();
        }

        public FreeSpaceHandlingDisabler Disable()
        {
            DisableStatus.DisableCount++;
            return DisableStatus;
        }

        private static FixedSizeTree GetFreeSpaceTree(LowLevelTransaction tx)
        {
            return new FixedSizeTree(tx, tx.RootObjects, FreeSpaceKey, 260)
            {
                FreeSpaceTree =true
            };
        }
    }
}
