// -----------------------------------------------------------------------
//  <copyright file="ParentPageAction.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Diagnostics;
using System.Linq;
using System.Runtime.CompilerServices;
using Voron.Global;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public sealed unsafe class ParentPageAction
    {
        private readonly TreePage _currentPage;
        private readonly TreePage _parentPage;
        private readonly Tree _tree;
        private readonly TreeCursor _cursor;
        private readonly LowLevelTransaction _tx;

        public ParentPageAction(TreePage parentPage, TreePage currentPage, Tree tree, TreeCursor cursor, LowLevelTransaction tx)
        {
            _parentPage = parentPage;
            _currentPage = currentPage;
            _tree = tree;
            _cursor = cursor;
            _tx = tx;
        }

        public TreePage ParentOfAddedPageRef { get; private set; }

        public byte* AddSeparator(Slice separator, long pageRefNumber, int? nodePos = null)
        {
            var originalLastSearchPositionOfParent = _parentPage.LastSearchPosition;

            if (nodePos == null)
                nodePos = _parentPage.NodePositionFor(_tx, separator); // select the appropriate place for this

            if (_parentPage.HasSpaceFor(_tx, TreeSizeOf.BranchEntry(separator) + Constants.Tree.NodeOffsetSize) == false)
            {
                var pageSplitter = new TreePageSplitter(_tx, _tree, separator, -1, pageRefNumber, TreeNodeFlags.PageRef, _cursor);

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

                Debug.Assert(_cursor.CurrentPage.GetNode(_cursor.CurrentPage.LastSearchPosition)->PageNumber == _currentPage.PageNumber, 
                            "The parent page is not referencing a page which is being split");
                Debug.Assert(Enumerable.Range(0, ParentOfAddedPageRef.NumberOfEntries).Any(i => ParentOfAddedPageRef.GetNode(i)->PageNumber == pageRefNumber),
                            "The parent page of a page reference isn't referencing it");

                return posToInsert;
            }

            ParentOfAddedPageRef = _parentPage;

            var pos = _parentPage.AddPageRefNode(nodePos.Value, separator, pageRefNumber);

            EnsureValidLastSearchPosition(_parentPage, _currentPage.PageNumber, originalLastSearchPositionOfParent);

            return pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void EnsureValidLastSearchPosition(TreePage page, long referencedPageNumber, int originalLastSearchPosition)
        {
            if (page.NumberOfEntries <= originalLastSearchPosition || page.GetNode(originalLastSearchPosition)->PageNumber != referencedPageNumber)
                page.LastSearchPosition = page.NodePositionReferencing(referencedPageNumber);
            else
                page.LastSearchPosition = originalLastSearchPosition;
        }
    }
}
