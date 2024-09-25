using System;
using System.Collections.Generic;
using Sparrow.Server;
using Voron.Data.Fixed;

namespace Voron.Impl.FreeSpace
{
    public sealed unsafe class FreeSpaceHandling : IFreeSpaceHandling
    {
        private static readonly Slice FreeSpaceKey;

        private readonly FreeSpaceHandlingDisabler _disableStatus = new FreeSpaceHandlingDisabler();

        private readonly FreeSpaceRecursiveCallGuard _guard;

        static FreeSpaceHandling()
        {
            using (StorageEnvironment.GetStaticContext(out var ctx))
            {
                Slice.From(ctx, "$free-space", ByteStringType.Immutable, out FreeSpaceKey);
            }
        }

        public FreeSpaceHandling()
        {
            _guard = new FreeSpaceRecursiveCallGuard(this);
        }

        internal const int NumberOfPagesInSection = 2048;

        public static bool IsFreeSpaceTreeName(string name)
        {
            return name == "$free-space";
        }

        public event Action<long> PageFreed;

        public long? TryAllocateFromFreeSpace(LowLevelTransaction tx, int num)
        {
            if (tx.RootObjects == null)
                return null;

            if (_disableStatus.DisableCount > 0)
                return null;

            if (_guard.IsProcessingFixedSizeTree)
                return null;

            using (_guard.Enter(tx))
            {
                var freeSpaceTree = GetFreeSpaceTree(tx);

                if (freeSpaceTree.NumberOfEntries == 0)
                    return null;

                using (var it = freeSpaceTree.Iterate())
                {
                    if (it.Seek(0) == false)
                        return null;

                    if (num < NumberOfPagesInSection)
                    {
                        return TryFindSmallValue(tx, freeSpaceTree, it, num);
                    }
                    return TryFindLargeValue(tx, freeSpaceTree, it, num);
                }
            }
        }

        private long? TryFindLargeValue(LowLevelTransaction tx, FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it, int num)
        {
            int numberOfNeededFullSections = num / NumberOfPagesInSection;
            int numberOfExtraBitsNeeded = num % NumberOfPagesInSection;

            var info = new FoundSectionsInfo();
            do
            {
                var stream = it.CreateReaderForCurrent();
                {
                    var current = new StreamBitArray(stream.Base);
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

                    StreamBitArray next;
                    var nextSectionId = currentSectionId + 1;
                    Slice read;
                    using (freeSpaceTree.Read(nextSectionId, out read))
                    {
                        if (!read.HasValue)
                        {
                            //not a following next section
                            info.Clear();
                            continue;
                        }

                        next = new StreamBitArray(read.CreateReader().Base);
                    }

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
                        next.Write(freeSpaceTree,nextSectionId);
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

        private sealed class FoundSectionsInfo
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


        private long? TryFindSmallValue(LowLevelTransaction tx, FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it, int num)
        {
            do
            {
                var current = new StreamBitArray(it.CreateReaderForCurrent().Base);

                if (current.SetCount >= num &&
                    TryFindContinuousRange(freeSpaceTree, it, num, current, it.CurrentKey, out long? page))
                    return page;

                //could not find a continuous so trying to merge
                if (TryFindSmallValueMergingTwoSections(tx, freeSpaceTree, it.CurrentKey, num, current, out page))
                    return page;
            }
            while (it.MoveNext());

            return null;
        }

        private bool TryFindContinuousRange(FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it, int num,
            StreamBitArray current, long currentSectionId, out long? page)
        {
            page = -1;
            var start = -1;
            while(true)
            {
                start = current.FirstSetBit(start + 1);
                if (start == -1 || 
                    start + num > NumberOfPagesInSection)
                    return false;

                if (num > 1)
                {
                    var nextUnsetBit = current.NextUnsetBits(start + 1);
                    if (nextUnsetBit != -1 && (nextUnsetBit - start) < num )
                    {
                        start = nextUnsetBit;
                        continue;
                    }
                }                
                
                page = currentSectionId * NumberOfPagesInSection + start;
                break;
            }

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

                current.Write(freeSpaceTree, it.CurrentKey);
            }

            return true;
        }

        private static bool TryFindSmallValueMergingTwoSections(LowLevelTransaction tx, FixedSizeTree freeSpacetree, long currentSectionId, int num,
            StreamBitArray current, out long? result)
        {
            result = -1;
            var currentEndRange = current.GetEndRangeCount();
            if (currentEndRange == 0)
                return false;

            var nextSectionId = currentSectionId + 1;

            StreamBitArray next;
            using (freeSpacetree.Read(nextSectionId, out Slice read))
            {
                if (!read.HasValue)
                    return false;

                next = new StreamBitArray(read.CreateReader().Base);
            }

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

                next.Write(freeSpacetree, nextSectionId);
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

                current.Write(freeSpacetree, currentSectionId);
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

                    var current = new StreamBitArray(stream.Base);
                    var currentSectionId = it.CurrentKey;

                    for (var i = 0; i < NumberOfPagesInSection; i++)
                    {
                        if (current.Get(i))
                            freePages.Add(currentSectionId*NumberOfPagesInSection + i);
                    }
                } while (it.MoveNext());

                return freePages;
            }
        }

        public IEnumerable<long> GetAllFullyEmptySegments(LowLevelTransaction tx)
        {
            var freeSpaceTree = GetFreeSpaceTree(tx);
            using (var it = freeSpaceTree.Iterate())
            {
                if (it.Seek(0) == false)
                    yield break;

                do
                {
                    int freePagesInSegment = it.CreateReaderForCurrent().ReadLittleEndianInt32();
                    if (freePagesInSegment == NumberOfPagesInSection)
                    {
                        yield return it.CurrentKey * NumberOfPagesInSection;
                    }
                } while (it.MoveNext());
            }
        }

        public void FreePage(LowLevelTransaction tx, long pageNumber)
        {
            if (_guard.IsProcessingFixedSizeTree)
            {
                _guard.PagesFreed.Add(pageNumber);
                return;
            }
            using (_guard.Enter(tx))
            {
                var freeSpaceTree = GetFreeSpaceTree(tx);
                StreamBitArray sba;
                var section = pageNumber / NumberOfPagesInSection;
                using (freeSpaceTree.Read(section, out Slice result))
                {
                    sba = !result.HasValue ? new StreamBitArray() : new StreamBitArray(result.CreateReader().Base);
                }
                sba.Set((int)(pageNumber % NumberOfPagesInSection), true);
                if (sba.SetCount == NumberOfPagesInSection)
                {
                    tx.RecordSparseRange(section * NumberOfPagesInSection);
                }

                sba.Write(freeSpaceTree, section);

                PageFreed?.Invoke(pageNumber);
            }
        }

        public long GetFreePagesOverhead(LowLevelTransaction tx)
        {
            var fst = GetFreeSpaceTree(tx);
            return fst.PageCount;
        }

        public IEnumerable<long> GetFreePagesOverheadPages(LowLevelTransaction tx)
        {
            var fst = GetFreeSpaceTree(tx);
            foreach (var page in fst.AllPages())
            {
                yield return page;
            }
        }

        public FreeSpaceHandlingDisabler Disable()
        {
            _disableStatus.DisableCount++;
            return _disableStatus;
        }

        private static FixedSizeTree GetFreeSpaceTree(LowLevelTransaction tx)
        {
            if (tx._freeSpaceTree != null)
            {
                return tx._freeSpaceTree;
            }
            return tx._freeSpaceTree = new FixedSizeTree(tx, tx.RootObjects, FreeSpaceKey, 260, clone: false)
            {
                FreeSpaceTree = true
            };
        }
    }
}
