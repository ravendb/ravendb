using Voron.Global;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public sealed unsafe class TreeMutableState
    {
        private TreeRootHeader _header;
        private bool _stateIsModified;

        public ref readonly TreeRootHeader Header => ref _header;

        public bool IsModified => _stateIsModified;

        private readonly LowLevelTransaction _tx;

        public TreeMutableState(LowLevelTransaction tx)
        {
            _tx = tx;
        }

        internal TreeMutableState(LowLevelTransaction tx, TreeMutableState state)
        {
            _tx = tx;
            _header = state._header;
        }

        internal TreeMutableState(LowLevelTransaction tx, in TreeRootHeader header)
        {
            _tx = tx;
            _header = header;
        }

        internal ref TreeRootHeader Modify()
        {
            VoronExceptions.ThrowIfReadOnly(_tx);
            
            _stateIsModified = true;
            return ref _header;
        }

        public void CopyTo(TreeRootHeader* header)
        {
            *header = _header;
        }

        public void CopyTo(ref TreeRootHeader header)
        {
            header = _header;
        }

        public TreeMutableState Clone()
        {
            return new TreeMutableState(_tx, in _header);
        }

        public override string ToString()
        {
            return $@" Pages: {_header.PageCount:#,#}, Entries: {_header.NumberOfEntries:#,#}
    Depth: {_header.Depth}, FixedTreeFlags: {_header.Flags}
    Root Page: {_header.RootPageNumber}
    Leafs: {_header.LeafPages:#,#} Overflow: {_header.OverflowPages:#,#} Branches: {_header.BranchPages:#,#}
    Size: {((float)(_header.PageCount * Constants.Storage.PageSize) / (1024 * 1024)):F2} Mb";
        }
    }
}
