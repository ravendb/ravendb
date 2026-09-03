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
        private TreePage _parentPage;
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

        /// <summary>
        /// The parent page as this action left it. AddSeparator can move the page and fix up its
        /// search position, and TreePage is a value type, so the caller has to read it back.
        /// </summary>
        public TreePage ParentPage => _parentPage;

        public TreePage ParentOfAddedPageRef { get; private set; }

        public bool PerformedSplit { get; private set; }

        public byte* AddSeparator(Slice separator, long pageRefNumber, int? nodePos = null)
        {
            var originalLastSearchPositionOfParent = _parentPage.LastSearchPosition;

            if (_parentPage.HasSpaceFor(_tx, TreeSizeOf.BranchEntry(separator) + Constants.Tree.NodeOffsetSize) == false)
            {
                PerformedSplit = true;

                // the splitter decides the split shape based on _parentPage.LastSearchPosition - including
                // the sequential-insert optimization that appends without a key search - so it must reflect
                // the position of this separator instead of a leftover from the descent or another fix-up
                _parentPage.NodePositionFor(_tx, separator);
                // the splitter works off the cursor's copy of this page, so the position has to be
                // published there before it runs
                _cursor.SyncTopPage(_parentPage);

                var pageSplitter = new TreePageSplitter(_tx, _tree, separator, -1, pageRefNumber, TreeNodeFlags.PageRef, _cursor);

                var posToInsert = pageSplitter.Execute();

                ParentOfAddedPageRef = _cursor.CurrentPage;

                var adjustParentPageOnCursor = true;

                for (int i = 0; i < _cursor.CurrentPage.NumberOfEntries; i++)
                {
                    if (_cursor.CurrentPage.GetNode(i)->PageNumber == _currentPage.PageNumber)
                    {
                        adjustParentPageOnCursor = false;
                        _cursor.CurrentPageRef.LastSearchPosition = (short)i;
                        break;
                    }
                }

                if (adjustParentPageOnCursor)
                {
                    // the above page split has modified the cursor that its first page points to the parent of the leaf where 'separatorKey' was inserted
                    // and it doesn't have the reference to _page, we need to ensure that the actual parent is first at the cursor

                    _cursor.Pop();
                    _cursor.Push(_parentPage);

                    EnsureValidLastSearchPosition(ref _parentPage, _currentPage.PageNumber, originalLastSearchPositionOfParent);
                    _cursor.SyncTopPage(_parentPage);
                }

                Debug.Assert(_cursor.CurrentPage.GetNode(_cursor.CurrentPage.LastSearchPosition)->PageNumber == _currentPage.PageNumber, 
                            "The parent page is not referencing a page which is being split");
                Debug.Assert(Enumerable.Range(0, ParentOfAddedPageRef.NumberOfEntries).Any(i => ParentOfAddedPageRef.GetNode(i)->PageNumber == pageRefNumber),
                            "The parent page of a page reference isn't referencing it");

                return posToInsert;
            }

            ParentOfAddedPageRef = _parentPage;

            if (nodePos == null)
                nodePos = _parentPage.NodePositionFor(_tx, separator); // select the appropriate place for this

            var pos = _parentPage.AddPageRefNode(nodePos.Value, separator, pageRefNumber);

            EnsureValidLastSearchPosition(ref _parentPage, _currentPage.PageNumber, originalLastSearchPositionOfParent);
            _cursor.SyncTopPage(_parentPage);

            return pos;
        }

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        private static void EnsureValidLastSearchPosition(ref TreePage page, long referencedPageNumber, int originalLastSearchPosition)
        {
            if (page.NumberOfEntries <= originalLastSearchPosition || page.GetNode(originalLastSearchPosition)->PageNumber != referencedPageNumber)
                page.LastSearchPosition = (short)page.NodePositionReferencing(referencedPageNumber);
            else
                page.LastSearchPosition = (short)originalLastSearchPosition;
        }
    }
}
