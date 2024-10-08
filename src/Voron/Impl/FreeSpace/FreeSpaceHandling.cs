using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Sparrow.Server;
using Voron.Data.Fixed;

namespace Voron.Impl.FreeSpace
{
    public sealed unsafe class FreeSpaceHandling : IFreeSpaceHandling
    {
        private static readonly Slice FreeSpaceKey;

        private readonly FreeSpaceHandlingDisabler _disableStatus = new();

        private readonly FreeSpaceRecursiveCallGuard _guard;

        private readonly Dictionary<long, int> _maxConsecutiveRange = new();

        public void OnRollback() => _maxConsecutiveRange.Clear();
        
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

                using var it = freeSpaceTree.Iterate();
                if (it.Seek(0) is false)
                {
                    return null;
                }

                return num switch
                {
                    1 => AllocateSinglePage(freeSpaceTree, it),
                    < NumberOfPagesInSection => TryFindSmallValue(freeSpaceTree, num, it),
                    _ => TryFindLargeValue(freeSpaceTree, num, it)
                };
            }
        }

        private static long? AllocateSinglePage(FixedSizeTree freeSpaceTree, FixedSizeTree.IFixedSizeIterator it)
        {
            var current = new StreamBitArray(it.CreateReaderForCurrent().Base);
            int idx = current.FirstSetBit();
            Debug.Assert(idx is not -1, "idx is not -1 - because we *must* have _a_ value there");
                
            current.Set(idx, false);
            long currentSectionId = it.CurrentKey;
            
            FlushBitmap(freeSpaceTree, current, currentSectionId);
            return currentSectionId * NumberOfPagesInSection + idx;
        }

        private long? TryFindLargeValue(FixedSizeTree freeSpaceTree, int num, FixedSizeTree<long>.IFixedSizeIterator it)
        {
            int numberOfNeededFullSections = num / NumberOfPagesInSection;
            int numberOfExtraBitsNeeded = num % NumberOfPagesInSection;

            var info = new FoundSectionsInfo();
            do
            {
                var stream = it.CreateReaderForCurrent();
                
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
                using (freeSpaceTree.Read(nextSectionId, out Slice read))
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
                
                next.Set(0, numberOfExtraBitsNeeded, false);
                FlushBitmap(freeSpaceTree, next, nextSectionId);
                
                foreach (var section in info.Sections)
                {
                    freeSpaceTree.Delete(section);
                }

                return info.StartSectionId * NumberOfPagesInSection;
            } while (it.MoveNext());

            return null;
        }

        private struct FoundSectionsInfo()
        {
            public List<long> Sections = new();

            public long StartSection = -1;

            public long? StartSectionId;

            public void Clear()
            {
                StartSection = -1;
                StartSectionId = null;
                Sections.Clear();
            }
        }


        private long? TryFindSmallValue(FixedSizeTree freeSpaceTree, int num, FixedSizeTree.IFixedSizeIterator it)
        {
            do
            {
                long currentSectionId = it.CurrentKey;
                if(_maxConsecutiveRange.TryGetValue(currentSectionId, out var knownMax) && knownMax >= num)
                    continue; // we know it isn't there, so we can safely skip it
                
                var current = new StreamBitArray(it.CreateReaderForCurrent().Base);

                if (current.SetCount >= num &&
                    TryFindContinuousRange(freeSpaceTree, num, current, currentSectionId, out long? page))
                    return page;

                if (knownMax == 0 || num < knownMax)
                {
                    // here we _know_ it can't fit anything larger, so we mark it for the next time
                    _maxConsecutiveRange[currentSectionId] = num;
                }

                //could not find a continuous so trying to merge
                if (TryFindSmallValueMergingTwoSections(freeSpaceTree, currentSectionId, num, current, out page))
                    return page;
                
            } while (it.MoveNext());

            return null;
        }

        private bool TryFindContinuousRange(FixedSizeTree freeSpaceTree, int num,
            StreamBitArray current, long currentSectionId, out long? page)
        {
            Debug.Assert(num > 1, "num > 1");
            
            page = -1;
            int start = current.FindRange(num);
            if (start == -1)
                return false;
            
            page = currentSectionId * NumberOfPagesInSection + start;

            current.Set(start, num, false);
            FlushBitmap(freeSpaceTree, current, currentSectionId);
            return true;
        }

        private static void FlushBitmap(FixedSizeTree freeSpaceTree, StreamBitArray current, long currentSectionId)
        {
            if (current.SetCount == 0)
            {
                freeSpaceTree.Delete(currentSectionId);
            }
            else
            {
                current.Write(freeSpaceTree, currentSectionId);
            }
        }

        private static bool TryFindSmallValueMergingTwoSections(FixedSizeTree freeSpaceTree, long currentSectionId, int num,
            StreamBitArray current, out long? result)
        {
            result = -1;
            var currentEndRange = current.GetEndRangeCount();
            if (currentEndRange == 0)
                return false;

            var nextSectionId = currentSectionId + 1;

            StreamBitArray next;
            using (freeSpaceTree.Read(nextSectionId, out Slice read))
            {
                if (!read.HasValue)
                    return false;

                next = new StreamBitArray(read.CreateReader().Base);
            }

            var nextRange = num - currentEndRange;
            if (next.HasStartRangeCount(nextRange) == false)
                return false;

            current.Set(NumberOfPagesInSection - currentEndRange, currentEndRange, false);
            next.Set(0, nextRange, false);
            
            FlushBitmap(freeSpaceTree, next, nextSectionId);
            FlushBitmap(freeSpaceTree, current, currentSectionId);
            
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
                    int freePagesInSegment = it.CreateReaderForCurrent().Read<int>();
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
                _maxConsecutiveRange.Remove(section);
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
