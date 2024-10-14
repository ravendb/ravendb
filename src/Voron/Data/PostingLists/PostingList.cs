using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Sparrow;
using Sparrow.Server;
using Sparrow.Server.Utils;
using Sparrow.Server.Utils.VxSort;
using Voron.Debugging;
using Voron.Global;
using Voron.Impl;
using Voron.Util;
using Voron.Util.PFor;

namespace Voron.Data.PostingLists
{
    public sealed unsafe class PostingList : IDisposable
    {
        public Slice Name;
        private readonly LowLevelTransaction _llt;
        private PostingListState _state;
        private UnmanagedSpan<PostingListCursorState> _stk;
        private int _pos = -1, _len;
        private ByteStringContext<ByteStringMemoryCache>.InternalScope _scope;

        public PostingListState State => _state;
        internal LowLevelTransaction Llt => _llt;

        private ContextBoundNativeList<long> _additions, _removals;

        public PostingList(LowLevelTransaction llt, Slice name, in PostingListState state)
        {
            if (state.RootObjectType != RootObjectType.Set)
                throw new InvalidOperationException($"Tried to open {name} as a set, but it is actually a " +
                                                    state.RootObjectType);
            Name = name;
            _llt = llt;
            _state = state;

            // PERF: We dont have the ability to dispose Set (because of how it is used) therefore,
            // we will just discard the memory as reclaiming it may be even more costly.  
            _scope = llt.Allocator.AllocateDirect(8 * sizeof(PostingListCursorState), out ByteString buffer);
            _stk = new UnmanagedSpan<PostingListCursorState>(buffer.Ptr, buffer.Size);
            if (llt.Flags == TransactionFlags.ReadWrite)
            {
                _additions = new(llt.Allocator, 1);
                _removals = new(llt.Allocator, 1);
            }
        }

        public static void Create(LowLevelTransaction tx, ref PostingListState state)
        {
            var newPage = tx.AllocatePage(1);
            PostingListLeafPage postingListLeafPage = new PostingListLeafPage(newPage);
            PostingListLeafPage.InitLeaf(postingListLeafPage.Header);
            state.RootObjectType = RootObjectType.Set;
            state.Depth = 1;
            state.BranchPages = 0;
            state.LeafPages = 1;
            state.RootPage = newPage.PageNumber;
        }
        
        public static void Create(LowLevelTransaction tx, ref PostingListState state, FastPForEncoder encoder)
        {
            var newPage = tx.AllocatePage(1);
            PostingListLeafPage leafPage = new(newPage);
            PostingListLeafPage.InitLeaf(leafPage.Header);
            state.RootObjectType = RootObjectType.Set;
            state.Depth = 1;
            state.BranchPages = 0;
            state.LeafPages = 1;
            state.RootPage = newPage.PageNumber;

            leafPage.AppendToNewPage(tx, encoder);
            state.NumberOfEntries += leafPage.Header->NumberOfEntries;

            if (encoder.Done == false) // we overflow and need to split excess to additional pages
            {
                var self = new PostingList(tx, Slices.Empty, state);
                self.FindPageFor(0);
                self.CreateRootPage();
                self.AddNewPageForTheExtras(encoder);
                state = self._state;
            }
        }

        public List<long> DumpAllValues()
        {
            var iterator = Iterate();
            Span<long> buffer = stackalloc long[1024];
            var results = new List<long>();
            while (iterator.Fill(buffer, out var read) && read != 0)
            {
                results.AddRange(buffer[0..read].ToArray());
            }

            return results;
        }

        public void Add(long value)
        {
            if (value < 0)
                throw new ArgumentOutOfRangeException(nameof(value), "Only positive values are allowed");
            Debug.Assert((value & 1) == 0);
            _additions.Add(value);
        }
        
        public void Remove(long value)
        {
            Debug.Assert((value & 1) == 0);
            // used for testing only!
            // we need to explicitly mark it with 0b001 so merging will be fast
            _removals.Add(value | 1); 
        }

        [Conditional("DEBUG")]
        public void Render()
        {
            DebugStuff.RenderAndShow(this);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private void ResizeCursorState()
        {
            _scope.Dispose();
            _scope = _llt.Allocator.Allocate(_stk.Length * 2 * sizeof(PostingListCursorState), out ByteString buffer);
            var newStk = new UnmanagedSpan<PostingListCursorState>(buffer.Ptr, buffer.Size);
            _stk.ToReadOnlySpan().CopyTo(newStk.ToSpan());
            _stk = newStk;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private PostingListCursorState* FindSmallestValue()
        {
            _pos = -1;
            _len = 0;
            PushPage(_state.RootPage);

            var state = _stk.GetAsPtr(_pos);
            while (state->IsLeaf == false)
            {
                var branch = new PostingListBranchPage(state->Page);

                // Until we hit a leaf, just take the left-most key and move on. 
                long nextPage = branch.GetPageByIndex(0);
                PushPage(nextPage);

                state = _stk.GetAsPtr(_pos);
            }

            return state;
        }

        [Conditional("DEBUG")]
        public void Verify()
        {
            PostingListCursorState root = new (Llt.GetPage(_state.RootPage));
            VerifyPage(root, long.MinValue, long.MaxValue);
        }

        [Conditional("DEBUG")]
        private void VerifyPage(PostingListCursorState s, long min, long maxExclusive)
        {
            if (s.IsLeaf)
            {
                var values = new PostingListLeafPage(s.Page).GetDebugOutput();
                ValidateValues(s.Page.PageNumber, min, maxExclusive, values);
            }
            else
            {
                var values = new PostingListBranchPage(s.Page).GetDebugOutput();
                ValidateValues(s.Page.PageNumber, min, maxExclusive, values.Select(x=>x.Key).ToList());
                for (int i = 0; i < values.Count; i++)
                {
                    var v = values[i];
                    var max = i < values.Count - 1 ? values[i + 1].Key : maxExclusive;
                    VerifyPage(new PostingListCursorState(Llt.GetPage(v.Page)), v.Key, max);
                }
            }
        }

        private static void ValidateValues(long pageNumber, long min, long maxExclusive, List<long> values)
        {
            var sorted = values.ToArray();
            Array.Sort(sorted);
            if (sorted.SequenceEqual(values) == false)
                throw new InvalidOperationException("Page " + pageNumber + " is not sorted");
            if (values[0] < min)                              
                throw new InvalidOperationException("Page " + pageNumber + " first value is beyond its range: " + values[0] + " vs " + min);
            if (values[^1] >= maxExclusive)                   
                throw new InvalidOperationException("Page " + pageNumber + " last value is beyond its range: " + values[^1] + " vs " + maxExclusive);
        }

        private void FindPageFor(long value)
        {
            _pos = -1;
            _len = 0;
            PushPage(_state.RootPage);
            ref var state = ref _stk[_pos];

            while (state.IsLeaf == false)
            {
                var branch = new PostingListBranchPage(state.Page);
                (long nextPage, state.LastSearchPosition, state.LastMatch) = branch.SearchPage(value);

                PushPage(nextPage);

                state = ref _stk[_pos];
            }
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PopPage()
        {
            _stk[_pos--] = default;
            _len--;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private void PushPage(long nextPage)
        {
            if (_pos + 1 >= _stk.Length) //  should never actually happen
                ResizeCursorState();

            Page page = _llt.GetPage(nextPage);
            _pos++; 
            
            var state = _stk.GetAsPtr(_pos);
            state->Page = page;
            state->LastMatch = 0;
            state->LastSearchPosition = 0;

            _len++;
        }

        public Iterator Iterate()
        {
            if (_additions.Count != 0 || _removals.Count != 0)
                throw new NotSupportedException("The set was modified, cannot read from it until is was committed");
            return new Iterator(this);
        }

        public struct Iterator 
        {
            private readonly PostingList _parent;
            private PostingListLeafPage.Iterator _it;

            public bool IsValid => _parent != null;

            public Iterator(PostingList parent)
            {
                _parent = parent;
                _it = new PostingListLeafPage.Iterator(parent._llt.Allocator);
                SeekSmallest();
            }

            private bool SeekSmallest()
            {
                // We need to find the long.MinValue therefore the fastest way is to always
                // take the left-most pointer on any branch node.
                var state = _parent.FindSmallestValue();

                var leafPage = new PostingListLeafPage(state->Page);
                leafPage.SetIterator(ref _it);
                return _parent.State.NumberOfEntries > 0;
            }

            /// <summary>
            /// Seeks elements that are equal or greater than the given parameter
            /// </summary>
            /// <param name="from">Minimum value of element to seek</param>
            /// <returns>
            /// False - no element equal to or greater than the given parameter exists
            /// True - otherwise
            /// </returns>
            public bool Seek(long from = long.MinValue)
            {
                _parent.FindPageFor(from);
                ref var state = ref _parent._stk[_parent._pos];
                var leafPage = new PostingListLeafPage(state.Page);

                leafPage.SetIterator(ref _it);
                return _it.SkipHint(from);
            }

            public bool Fill(Span<long> matches, out int total, long pruneGreaterThanOptimization = long.MaxValue)
            {
                // We will try to fill.
                total = 0;
                          
                while(total < matches.Length)
                {
                    var read = _it.Fill(matches.Slice(total), out bool hasPrunedResults,  pruneGreaterThanOptimization);
                    
                    // We haven't read anything, but we are not getting a pruned result.
                    if (read == 0 && hasPrunedResults == false)
                    {
                        var parent = _parent;
                        if (parent._pos == 0)
                            break;

                        parent.PopPage();

                        var llt = parent._llt;

                        while (true)
                        {
                            ref var state = ref parent._stk[_parent._pos];
                            state.LastSearchPosition++;
                            Debug.Assert(state.IsLeaf == false);
                            if (state.LastSearchPosition >= state.BranchHeader->NumberOfEntries)
                            {
                                if (parent._pos == 0)
                                    break;

                                parent.PopPage();
                                continue;
                            }

                            var branch = new PostingListBranchPage(state.Page);
                            (_, long pageNum) = branch.GetByIndex(state.LastSearchPosition);
                            var page = llt.GetPage(pageNum);
                            var header = (PostingListLeafPageHeader*)page.Pointer;

                            parent.PushPage(pageNum);

                            if (header->PostingListFlags == ExtendedPageType.PostingListBranch)
                            {
                                // we'll increment on the next
                                parent._stk[parent._pos].LastSearchPosition = -1;
                                continue;
                            }
                            new PostingListLeafPage(page).SetIterator(ref _it);
                            break;
                        }
                    }                        

                    total += read;

                    // We have reached the end by prunning.
                    if (hasPrunedResults)
                        break; // We are done.
                }

                return total != 0;
            }

            public void Reset()
            {
                throw new NotSupportedException();
            }
        }

        public List<long> AllPages()
        {
            var result = new List<long>();
            AddPage(_llt.GetPage(_state.RootPage));
            return result;

            void AddPage(Page p)
            {
                result.Add(p.PageNumber);
                var state = new PostingListCursorState { Page = p, };
                if (state.BranchHeader->SetFlags != ExtendedPageType.PostingListBranch)
                    return;
                
                var branch = new PostingListBranchPage(state.Page);
                foreach (var child in branch.GetAllChildPages())
                {
                    var childPage = _llt.GetPage(child);
                    AddPage(childPage);
                }
            }
        }

        public void Dispose()
        {
            _additions.Dispose();
            _removals.Dispose();
            _scope.Dispose();
        }

        public void PrepareForCommit()
        {
            if (_additions.Count > 1)
                _additions.Count = Sorting.SortAndRemoveDuplicates(_additions.RawItems, _additions.Count);
            if (_removals.Count > 1)
                _removals.Count = Sorting.SortAndRemoveDuplicates(_removals.RawItems, _removals.Count);

            var additionsCount = _additions.Count;
            var removalsCount = _removals.Count;

            var encoder = new FastPForEncoder(_llt.Allocator);
            var decoder = new FastPForDecoder(_llt.Allocator);

            var tempList = new ContextBoundNativeList<long>(_llt.Allocator);
            UpdateList(_additions.RawItems, additionsCount, _removals.RawItems, removalsCount, encoder, ref tempList, ref decoder);
            tempList.Dispose();

            decoder.Dispose();
            encoder.Dispose();
        }

        private void UpdateList(long* additions, int additionsCount, long* removals, int removalsCount, 
            FastPForEncoder encoder, ref ContextBoundNativeList<long> tempList, ref FastPForDecoder decoder)
        {
            tempList.Clear();

            while (additionsCount != 0 || removalsCount != 0)
            {
                bool hadRemovals = removalsCount > 0;
                long first = -1;
                if (additionsCount > 0)
                    first = additions[0];
                if (removalsCount > 0)
                    first = first == -1 ? removals[0] : Math.Min(removals[0], first);

                FindPageFor(first);
                long limit = NextParentLimit();
                ref var state = ref _stk[_pos];
                state.Page = _llt.ModifyPage(state.Page.PageNumber);
                var leafPage = new PostingListLeafPage(state.Page);

                _state.NumberOfEntries -= leafPage.Header->NumberOfEntries;

                leafPage.Update(_llt, encoder, ref tempList, ref additions, ref additionsCount, ref removals, ref removalsCount, limit);
                _state.NumberOfEntries += leafPage.Header->NumberOfEntries;

                if (encoder.Done == false) // we overflow and need to split excess to additional pages
                {
                    AddNewPageForTheExtras(encoder);
                }
                else if (hadRemovals && _len > 1 && leafPage.SpaceUsed < Constants.Storage.PageSize / 2)
                {
                    // check if we want to merge this page
                    PopPage();

                    ref var parent = ref _stk[_pos];

                    var branch = new PostingListBranchPage(parent.Page);
                    Debug.Assert(branch.Header->NumberOfEntries >= 2);
                    var siblingIdx = GetSiblingIndex(parent);
                    var (_, siblingPageNum) = branch.GetByIndex(siblingIdx);

                    var siblingPage = _llt.ModifyPage(siblingPageNum);
                    var siblingHeader = (PostingListLeafPageHeader*)siblingPage.Pointer;
                    if (siblingHeader->PostingListFlags != ExtendedPageType.PostingListLeaf)
                        continue;

                    var sibling = new PostingListLeafPage(siblingPage);
                    if (sibling.SpaceUsed + leafPage.SpaceUsed > Constants.Storage.PageSize / 2 + Constants.Storage.PageSize / 4)
                    {
                        // if the two pages together will be bigger than 75%, can skip merging
                        // we do that to prevent "jumping" around between adding a page & removing that
                        continue;
                    }

                    // we assume that the two pages can be merged, note that they don't *have* to
                    // we do a quick validation above, but we don't check if shared prefixes, etc are involved
                    if (PostingListLeafPage.TryMerge(_llt, _llt.Allocator, ref decoder, leafPage.Header,
                            parent.LastSearchPosition == 0 ? leafPage.Header : siblingHeader,
                            parent.LastSearchPosition == 0 ? siblingHeader : leafPage.Header
                        ))
                    {
                        MergeSiblingsAtParent();
                    }

                }
            }
        }

        private static int GetSiblingIndex(in PostingListCursorState parent)
        {
            return parent.LastSearchPosition == 0 ? 1 : parent.LastSearchPosition - 1;
        }
        
        private void MergeSiblingsAtParent()
        {
            ref var state = ref _stk[_pos];
            state.Page = _llt.ModifyPage(state.Page.PageNumber);
            
            var current = new PostingListBranchPage(state.Page);
            Debug.Assert(current.Header->SetFlags == ExtendedPageType.PostingListBranch);
            var (siblingKey, siblingPageNum) = current.GetByIndex(GetSiblingIndex(in state));
            var (leafKey, leafPageNum) = current.GetByIndex(state.LastSearchPosition);

            var siblingPageHeader = (PostingListLeafPageHeader*)_llt.GetPage(siblingPageNum).Pointer;
            if (siblingPageHeader->PostingListFlags == ExtendedPageType.PostingListBranch)
                _state.BranchPages--;
            else
                _state.LeafPages--;
            
            _llt.FreePage(siblingPageNum);
            current.Remove(siblingKey);
            current.Remove(leafKey);

            // if it is empty, can just replace with the child
            if (current.Header->NumberOfEntries == 0)
            {
                var leafPage = _llt.GetPage(leafPageNum);
                
                long cpy = state.Page.PageNumber;
                leafPage.CopyTo(state.Page);
                state.Page.PageNumber = cpy;

                if (_pos == 0)
                    _state.Depth--; // replaced the root page

                _state.BranchPages--;
                _llt.FreePage(leafPageNum);
                return;
            }

            var newKey = Math.Min(siblingKey, leafKey);
            if (current.TryAdd(_llt, newKey, leafPageNum) == false)
                throw new InvalidOperationException("We just removed two values to add one, should have enough space. This error should never happen");

            Debug.Assert(current.Header->NumberOfEntries >= 2);
            
            if (_pos == 0)
                return; // root has no siblings

            if (current.Header->NumberOfEntries > PostingListBranchPage.MinNumberOfValuesBeforeMerge)
                return;

            PopPage();
            ref var parent = ref _stk[_pos];
            
            var gp = new PostingListBranchPage(parent.Page);
            var siblingIdx = GetSiblingIndex(parent);
            (_, siblingPageNum) = gp.GetByIndex(siblingIdx);
            var siblingPage = _llt.GetPage(siblingPageNum);
            var siblingHeader = (PostingListLeafPageHeader*)siblingPage.Pointer;
            if (siblingHeader->PostingListFlags != ExtendedPageType.PostingListBranch)
                return;// cannot merge leaf & branch
            
            var sibling = new PostingListBranchPage(siblingPage);
            if (sibling.Header->NumberOfEntries + current.Header->NumberOfEntries > PostingListBranchPage.MinNumberOfValuesBeforeMerge * 2)
                return; // not enough space to _ensure_ that we can merge

            for (int i = 0; i < sibling.Header->NumberOfEntries; i++)
            {
                (long key, long page) = sibling.GetByIndex(i);
                if(current.TryAdd(_llt, key, page) == false)
                    throw new InvalidOperationException("Even though we have checked for spare capacity, we run out?! Should not happen ever");
            }

            MergeSiblingsAtParent();
        }

        private void AddNewPageForTheExtras(FastPForEncoder encoder)
        {
            while (encoder.Done == false)
            {
                var newPage = new PostingListLeafPage(_llt.AllocatePage(1));
                PostingListLeafPage.InitLeaf(newPage.Header);
                _state.LeafPages++;
                var first = encoder.NextValueToEncode;
                newPage.AppendToNewPage(_llt, encoder);
                _state.NumberOfEntries += newPage.Header->NumberOfEntries;
                AddToParentPage(first, newPage.Header->PageNumber);
            }
        }

        private long NextParentLimit()
        {
            var cur = _pos;
            while (cur > 0)
            {
                ref var state = ref _stk[cur - 1];
                if (state.LastSearchPosition + 1 < state.BranchHeader->NumberOfEntries)
                {
                    var (key, _) = new PostingListBranchPage(state.Page).GetByIndex(state.LastSearchPosition + 1);
                    return key;
                }
                cur--;
            }
            return long.MaxValue;
        }
        
        private void AddToParentPage(long separator, long newPage)
        {
            if (_pos == 0) // need to create a root page
            {
                var root = CreateRootPage();
                if(root.TryAdd(_llt, separator, newPage) == false)
                    throw new InvalidOperationException("Failed to add to a newly created ROOT page? Should never happen");
                return;
            }

            PopPage();
            ref var state = ref _stk[_pos];
            Debug.Assert(state.IsLeaf == false);
            state.Page = _llt.ModifyPage(state.Page.PageNumber);
            var parent = new PostingListBranchPage(state.Page);
            if (parent.TryAdd(_llt, separator, newPage))
            {
                PushPage(newPage);
                return;
            }

            SplitBranchPage(separator, newPage);
        }

        private void SplitBranchPage(long key, long value)
        {
            ref var state = ref _stk[_pos];

            // Create a new branch page to split the existing page
            var pageToSplit = new PostingListBranchPage(state.Page);
            var page = _llt.AllocatePage(1);
            var branch = new PostingListBranchPage(page);
            branch.Init();
            _state.BranchPages++;
            
            // grow rightward:
            int rightMostSectionToMove = pageToSplit.Header->NumberOfEntries / 2;
            if (key > pageToSplit.Last)
            {
                // If the key is greater than the last key in the page to split,
                // we'll move a single entry from the current page to the new branch page
                // this ensures that we always have branches with at least 2 leaves
                rightMostSectionToMove = pageToSplit.Header->NumberOfEntries - 1;
            }

            // split in half
            // add the upper half of the entries to the new page 
            for (int i = rightMostSectionToMove; i < pageToSplit.Header->NumberOfEntries; i++)
            {
                var (k, v) = pageToSplit.GetByIndex(i);
                if(branch.TryAdd(_llt, k, v) == false)
                    throw new InvalidOperationException("Failed to add half our capacity to a newly created page? Should never happen");
            }

            pageToSplit.Header->NumberOfEntries = (ushort)rightMostSectionToMove;// truncate entries
            var success = pageToSplit.Last < key ?
                branch.TryAdd(_llt, key, value) :
                pageToSplit.TryAdd(_llt, key, value);
            if(success == false)
                throw new InvalidOperationException("Failed to add final to a newly created page after adding half the capacit? Should never happen");

            AddToParentPage(branch.First, page.PageNumber);
            // we need to position the cursor so we'll have the _next_ addition on the new branch page, not on the root
            FindPageFor(branch.First);
        }
        
        private void InsertToStack(PostingListCursorState newPageState)
        {
            // insert entry and shift other elements
            if (_len + 1 >= _stk.Length) // should never happen
                ResizeCursorState();

            var src = _stk.ToReadOnlySpan().Slice(_pos + 1, _len - (_pos + 1));
            var dest = _stk.ToSpan().Slice(_pos + 2);
            src.CopyTo(dest);

            _len++;
            _stk[_pos + 1] = newPageState;
            _pos++;
        }

        private PostingListBranchPage CreateRootPage()
        {
            _state.Depth++;
            _state.BranchPages++;
            // we'll copy the current page and reuse it, to avoid changing the root page number
            var page = _llt.AllocatePage(1);
            long cpy = page.PageNumber;
            ref var state = ref _stk[_pos];
            Debug.Assert(_llt.IsDirty(page.PageNumber));
            Memory.Copy(page.Pointer, state.Page.Pointer, Constants.Storage.PageSize);
            page.PageNumber = cpy;
            Debug.Assert(_llt.IsDirty(state.Page.PageNumber));
            Memory.Set(state.Page.DataPointer, 0, Constants.Storage.PageSize - PageHeader.SizeOf);
            var rootPage = new PostingListBranchPage(state.Page);
            rootPage.Init();
            rootPage.TryAdd(_llt, long.MinValue, cpy);

            InsertToStack(state with { Page = page });
            state.LastMatch = -1;
            state.LastSearchPosition = 0;
            return rootPage;
        }

        public static long Update(LowLevelTransaction transactionLowLevelTransaction, ref PostingListState postingListState,
            long* additions, int additionsCount, long* removals, int removalsCount, FastPForEncoder encoder, ref ContextBoundNativeList<long> tempList, ref FastPForDecoder decoder)
        {
            using var pl = new PostingList(transactionLowLevelTransaction, Slices.Empty, postingListState);
            pl.UpdateList(additions, additionsCount, removals, removalsCount, encoder, ref tempList, ref decoder);
            postingListState = pl.State;

            return pl.State.NumberOfEntries;
        }

        public static void SortEntriesAndRemoveDuplicatesAndRemovals(ref ContextBoundNativeList<long> list)
        {
            if (list.Count <= 1)
                return;

            Sort.Run(list.RawItems, list.Count);

            // blog post explaining this
            // https://ayende.com/blog/200065-B/optimizing-a-three-way-merge?key=67d6f65d63ba4fb79d31dfc49ae5aa1d

            // The idea here is that we can do all of the process with no branches at all and make this 
            // easily predictable to the CPU

            // Here we rely on the fact that the removals has been set with 1 at the bottom bit
            // so existing / additions values would always sort *before* the removals
            var outputBufferPtr = list.RawItems;

            var bufferPtr = outputBufferPtr;
            var bufferEndPtr = bufferPtr + list.Count - 1;
            Debug.Assert((*bufferPtr & 1) == 0,
                "Removal as first item means that we have an orphaned removal, not supposed to happen!");
            while (bufferPtr < bufferEndPtr)
            {
                // here we check equality without caring if this is removal or not, skipping moving
                // to the next value if this it is the same entry twice
                outputBufferPtr += ((bufferPtr[1] & ~1) != bufferPtr[0]).ToInt32();
                *outputBufferPtr = bufferPtr[1];
                // here we check if the entry is a removal, in which can we _decrement_ the position
                // in effect, removing it
                outputBufferPtr -= (bufferPtr[1] & 1);

                bufferPtr++;
            }

            list.Count = (int)(outputBufferPtr - list.RawItems + 1);
        }
    }
}
