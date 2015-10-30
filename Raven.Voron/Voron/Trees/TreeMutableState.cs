using System;
using System.Diagnostics;
using Voron.Impl.FileHeaders;
using Voron.Impl.Paging;

namespace Voron.Trees
{
    public unsafe class TreeMutableState
    {
        public long BranchPages;
        public long LeafPages;
        public long OverflowPages;
        public int Depth;
        public long PageCount;
        public long EntriesCount;
        public TreeFlags Flags;
        public bool KeysPrefixing;

        public long RootPageNumber;
        private bool _isModified;

        public bool InWriteTransaction;

        public bool IsModified
        {
            get { return _isModified; }
            set
            {
                if (InWriteTransaction == false)
                    throw new InvalidOperationException("Invalid operation outside of a write transaction");
                _isModified = value;
            }
        }

        public void CopyTo(TreeRootHeader* header)
        {
            header->Flags = Flags;
            header->BranchPages = BranchPages;
            header->Depth = Depth;
            header->LeafPages = LeafPages;
            header->OverflowPages = OverflowPages;
            header->PageCount = PageCount;
            header->EntriesCount = EntriesCount;
            header->RootPageNumber = RootPageNumber;
            header->KeysPrefixing = KeysPrefixing;
        }

        public TreeMutableState Clone()
        {
            return new TreeMutableState
                {
                    BranchPages = BranchPages,
                    Depth = Depth,
                    EntriesCount = EntriesCount,
                    LeafPages = LeafPages,
                    OverflowPages = OverflowPages,
                    PageCount = PageCount,
                    Flags = Flags,
                    RootPageNumber = RootPageNumber,
                    KeysPrefixing = KeysPrefixing
                };
        }

        public void RecordNewPage(Page p, int num)
        {
            PageCount += num;

            if (p.IsBranch)
            {
                BranchPages++;
            }
            else if (p.IsLeaf)
            {
                LeafPages++;
            }
            else if (p.IsOverflow)
            {
                OverflowPages += num;
            }
        }

        public void RecordFreedPage(Page p, int num)
        {
            PageCount -= num;
            Debug.Assert(PageCount >= 0);

            if (p.IsBranch)
            {
                BranchPages--;
                Debug.Assert(BranchPages >= 0);
            }
            else if (p.IsLeaf)
            {
                LeafPages--;
                Debug.Assert(LeafPages >= 0);
            }
            else if (p.IsOverflow)
            {
                OverflowPages -= num;
                Debug.Assert(OverflowPages >= 0);
            }
        }

        public override string ToString()
        {
            return string.Format(@" Pages: {1:#,#}, Entries: {2:#,#}
    Depth: {0}, Flags: {3}
    Root Page: {4}
    Leaves: {5:#,#} Overflow: {6:#,#} Branches: {7:#,#}
    Size: {8:F2} Mb", Depth, PageCount, EntriesCount, Flags, RootPageNumber, LeafPages, OverflowPages, BranchPages, ((float)(PageCount * AbstractPager.PageSize) / (1024 * 1024)));
        }
    }
}
