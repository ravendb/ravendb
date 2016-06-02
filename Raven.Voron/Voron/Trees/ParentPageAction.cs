// -----------------------------------------------------------------------
//  <copyright file="ParentPageAction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
#if VALIDATE
using System.Diagnostics;
using System.Linq;
#endif
using System.Runtime.CompilerServices;
using Voron.Impl;

namespace Voron.Trees
{
    public unsafe class ParentPageAction
    {
        private readonly Page _currentPage;
        private readonly Page _parentPage;
        private readonly Tree _tree;
        private readonly Cursor _cursor;
        private readonly Transaction _tx;

        public ParentPageAction(Page parentPage, Page currentPage, Tree tree, Cursor cursor, Transaction tx)
        {
            _parentPage = parentPage;
            _currentPage = currentPage;
            _tree = tree;
            _cursor = cursor;
            _tx = tx;
        }

        public Page ParentOfAddedPageRef { get; private set; }

        public byte* AddSeparator(MemorySlice separator, long pageRefNumber, int? nodePos = null)
        {
            var originalLastSearchPositionOfParent = _parentPage.LastSearchPosition;

            if (nodePos == null)
                nodePos = _parentPage.NodePositionFor(separator); // select the appropriate place for this

            var separatorKeyToInsert = _parentPage.PrepareKeyToInsert(separator, nodePos.Value);

            if (_parentPage.HasSpaceFor(_tx, SizeOf.BranchEntry(separatorKeyToInsert) + Constants.NodeOffsetSize + SizeOf.NewPrefix(separatorKeyToInsert)) == false)
            {
                var pageSplitter = new PageSplitter(_tx, _tree, separator, -1, pageRefNumber, NodeFlags.PageRef, 0, _cursor);

                var posToInsert = pageSplitter.Execute();

                ParentOfAddedPageRef = _cursor.CurrentPage;

                var adjustParentPageOnCursor = true;

                for (int i = 0; i < _cursor.CurrentPage.NumberOfEntries; i++)
                {
                    if (_cursor.CurrentPage.GetNode(i)->PageNumber == _currentPage.PageNumber)
                    {
                        adjustParentPageOnCursor = false;
                        _cursor.CurrentPage.LastSearchPosition = i;
                        break;
                    }
                }

                if (adjustParentPageOnCursor)
                {
                    // the above page split has modified the cursor that its first page points to the parent of the leaf where 'separatorKey' was inserted
                    // and it doesn't have the reference to _page, we need to ensure that the actual parent is first at the cursor

                    _cursor.Pop();
                    _cursor.Push(_parentPage);

                    EnsureValidLastSearchPosition(_parentPage, _currentPage.PageNumber, originalLastSearchPositionOfParent);
                }
#if VALIDATE
                Debug.Assert(_cursor.CurrentPage.GetNode(_cursor.CurrentPage.LastSearchPosition)->PageNumber == _currentPage.PageNumber, 
                            "The parent page is not referencing a page which is being split");

                var parentToValidate = ParentOfAddedPageRef;
                Debug.Assert(Enumerable.Range(0, parentToValidate.NumberOfEntries).Any(i => parentToValidate.GetNode(i)->PageNumber == pageRefNumber),
                            "The parent page of a page reference isn't referencing it");
#endif


                return posToInsert;
            }

            ParentOfAddedPageRef = _parentPage;

            var pos = _parentPage.AddPageRefNode(nodePos.Value, separatorKeyToInsert, pageRefNumber);

            EnsureValidLastSearchPosition(_parentPage, _currentPage.PageNumber, originalLastSearchPositionOfParent);
            
            return pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void EnsureValidLastSearchPosition(Page page, long referencedPageNumber, int originalLastSearchPosition)
        {
            if (page.NumberOfEntries <= originalLastSearchPosition || page.GetNode(originalLastSearchPosition)->PageNumber != referencedPageNumber)
                page.LastSearchPosition = page.NodePositionReferencing(referencedPageNumber);
            else
                page.LastSearchPosition = originalLastSearchPosition;
        }
    }
}