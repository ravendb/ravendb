using System;
using System.Collections.Generic;
using Sparrow.Json.Parsing;
using Sparrow.Server;
using Voron.Data.Fixed;

namespace Voron.Impl.FreeSpace
{
    public sealed class FreeSpaceHandling : IFreeSpaceHandling
    {
        private static readonly Slice FreeSpaceKey;

        private readonly FreeSpaceHandlingDisabler _disableStatus = new FreeSpaceHandlingDisabler();

        private readonly FreeSpaceRecursiveCallGuard _guard;

        private readonly Dictionary<long, int> _maxConsecutiveRangePerSection = new();

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

                        next = new StreamBitArray(read.CreateReader());
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
                        Slice val;
                        using (next.ToSlice(tx.Allocator, out val))
                            freeSpaceTree.Add(nextSectionId, val);
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
                var current = new StreamBitArray(it.CreateReaderForCurrent());

                long currentSectionId = it.CurrentKey;
                long? page;
                if (_maxConsecutiveRangePerSection.TryGetValue(currentSectionId, out var knownMax) && knownMax <= num)
                {
                    // current section's maximum continuous range is insufficient for the requested size.
                    // while we can skip searching within this section alone, we still need to check
                    // if merging with the following section could provide enough space
                    if (TryFindSmallValueMergingTwoSections(tx, freeSpaceTree, currentSectionId, num, current, out page))
                        return page;
                    
                    continue;
                }

                if (current.SetCount >= num
                    && TryFindContinuousRange(tx, freeSpaceTree, it, num, current, currentSectionId, out page))
                    return page;

                if (knownMax == 0 || num < knownMax)
                {
                    // here we _know_ it can't fit anything larger, so we mark it for the next time
                    _maxConsecutiveRangePerSection[currentSectionId] = num;
                }

                // could not find a continuous so trying to merge two consecutive sections
                if (TryFindSmallValueMergingTwoSections(tx, freeSpaceTree, currentSectionId, num, current, out page))
                    return page;
            }
            while (it.MoveNext());

            return null;
        }

        private bool TryFindContinuousRange(LowLevelTransaction tx, FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it, int num,
            StreamBitArray current, long currentSectionId, out long? page)
        {
            page = -1;

            var start = current.GetContinuousRangeStart(num);
            if (start == null)
                return false;

            page = currentSectionId * NumberOfPagesInSection + start;

            if (current.SetCount == num)
            {
                freeSpaceTree.Delete(it.CurrentKey);
            }
            else
            {
                for (int i = 0; i < num; i++)
                {
                    current.Set(i + start.Value, false);
                }

                Slice val;
                using (current.ToSlice(tx.Allocator, out val))
                    freeSpaceTree.Add(it.CurrentKey, val);
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
            Slice read;
            using (freeSpacetree.Read(nextSectionId, out read))
            {
                if (!read.HasValue)
                    return false;

                next = new StreamBitArray(read.CreateReader());
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
                Slice val;
                using (next.ToSlice(tx.Allocator, out val))
                    freeSpacetree.Add(nextSectionId, val);
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
                Slice val;
                using (current.ToSlice(tx.Allocator, out val))
                    freeSpacetree.Add(currentSectionId, val);
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
                            freePages.Add(currentSectionId*NumberOfPagesInSection + i);
                    }
                } while (it.MoveNext());

                return freePages;
            }
        } 
        public List<DynamicJsonValue> FreeSpaceSnapshot(LowLevelTransaction tx, bool hex)
        {
            var freeSpaceTree = GetFreeSpaceTree(tx);
            if (freeSpaceTree.NumberOfEntries == 0)
                return new List<DynamicJsonValue>();

            using (var it = freeSpaceTree.Iterate())
            {
                if (it.Seek(0) == false)
                    return new List<DynamicJsonValue>();

                var freeSpace = new List<DynamicJsonValue>();

                do
                {
                    var stream = it.CreateReaderForCurrent();
                    var current = new StreamBitArray(stream);
                    freeSpace.Add(current.ToJson(it.CurrentKey, hex));
                } while (it.MoveNext());

                return freeSpace;
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

                _maxConsecutiveRangePerSection.Remove(section);

                using (freeSpaceTree.Read(section, out Slice result))
                {
                    sba = !result.HasValue ? new StreamBitArray() : new StreamBitArray(result.CreateReader());
                }
                sba.Set((int)(pageNumber % NumberOfPagesInSection), true);

                Slice val;
                using (sba.ToSlice(tx.Allocator, out val))
                    freeSpaceTree.Add(section, val);

                var onPageFreed = PageFreed;
                onPageFreed?.Invoke(pageNumber);
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

        public Dictionary<long, int> GetMaxConsecutiveRangePerSection(LowLevelTransaction tx)
        {
            if (tx.Transaction.IsWriteTransaction == false)
                throw new InvalidOperationException($"{nameof(GetMaxConsecutiveRangePerSection)} must be called with an open write transaction");

            return new Dictionary<long, int>(_maxConsecutiveRangePerSection);
        }

        public void OnRollback() => _maxConsecutiveRangePerSection.Clear();

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
