using System;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public unsafe class TreePageIterator : IIterator
    {
        private readonly Slice _treeKey;
        private readonly Tree _parent;
        private readonly TreePage _page;
        private readonly LowLevelTransaction _tx;

        private Slice _currentKey = default(Slice);
        private Slice _currentInternalKey = default(Slice);
        private bool _disposed;

        public TreePageIterator(LowLevelTransaction tx, Slice treeKey, Tree parent, TreePage page)
        {
            _tx = tx;
            _treeKey = treeKey;
            _parent = parent;
            _page = page;
        }

        public void Dispose()
        {
            _disposed = true;

            OnDisposal?.Invoke(this);
        }

        public bool Seek(Slice key)
        {
            if(_disposed)
                throw new ObjectDisposedException("PageIterator");
            var current = _page.Search(_tx, key);
            if (current == null)
                return false;

            _currentInternalKey = TreeNodeHeader.ToSlicePtr(_tx.Allocator, current);
            _currentKey = _currentInternalKey;

            if (DoRequireValidation)
                return this.ValidateCurrentKey(_tx, current);

            return true;
        }

        public TreeNodeHeader* Current
        {
            get
            {

                if (_disposed)
                    throw new ObjectDisposedException("PageIterator");
                if (_page.LastSearchPosition< 0  || _page.LastSearchPosition >= _page.NumberOfEntries)
                    throw new InvalidOperationException("No current page was set");
                return _page.GetNode(_page.LastSearchPosition);
            }
        }


        public Slice CurrentKey
        {
            get
            {

                if (_disposed)
                    throw new ObjectDisposedException("PageIterator");
                if (_page.LastSearchPosition < 0 || _page.LastSearchPosition >= _page.NumberOfEntries)
                    throw new InvalidOperationException("No current page was set");
                return _currentKey;
            }
        }
        public int GetCurrentDataSize()
        {
            if (_disposed)
                throw new ObjectDisposedException("PageIterator");
            return Current->DataSize;
        }


       private bool _requireValidation;
        public bool DoRequireValidation
        {
            get { return _requireValidation; }
        }

        private Slice _requiredPrefix;
        public Slice RequiredPrefix
        {
            get { return _requiredPrefix; }
            set
            {
                _requiredPrefix = value;
                _requireValidation = _maxKey.HasValue || _requiredPrefix.HasValue;
            }
        }

        private Slice _maxKey;
        public Slice MaxKey
        {
            get { return _maxKey; }
            set
            {
                _maxKey = value;
                _requireValidation = _maxKey.HasValue || _requiredPrefix.HasValue;
            }
        }

        public bool MoveNext()
        {
            _page.LastSearchPosition++;
            return TrySetPosition();
        }

        public bool MovePrev()
        {
            _page.LastSearchPosition--;

            return TrySetPosition();

        }

        public bool Skip(int count)
        {
            _page.LastSearchPosition += count;
            
            return TrySetPosition();
        }

        private bool TrySetPosition()
        {

            if (_disposed)
                throw new ObjectDisposedException("PageIterator");
            if (_page.LastSearchPosition < 0 || _page.LastSearchPosition >= _page.NumberOfEntries)
                return false;

            var current = _page.GetNode(_page.LastSearchPosition);
            if (DoRequireValidation && this.ValidateCurrentKey(_tx, current) == false)
            {
                return false;
            }

            _currentInternalKey = TreeNodeHeader.ToSlicePtr(_tx.Allocator, current);
            _currentKey = _currentInternalKey;
            return true;
        }

        public ValueReader CreateReaderForCurrent()
        {
            var node = Current;
            return new ValueReader((byte*)node + node->KeySize + Constants.NodeHeaderSize, node->DataSize);
        }

        public event Action<IIterator> OnDisposal;
    }
}
