// -----------------------------------------------------------------------
//  <copyright file="Iterators.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using Sparrow;
using System;
using Voron.Impl;
using Voron.Impl.FileHeaders;

namespace Voron.Data.Fixed
{
    public unsafe partial class FixedSizeTree
    {
        public interface IFixedSizeIterator : IDisposable
        {
            bool SeekToLast();
            bool Seek(long key);
            long CurrentKey { get; }
            ByteStringContext.Scope Value(out Slice slice);
            bool MoveNext();
            bool MovePrev();

            ValueReader CreateReaderForCurrent();

            bool Skip(int count);
        }

        public class NullIterator : IFixedSizeIterator
        {
            public bool SeekToLast()
            {
                return false;
            }

            public bool Seek(long key)
            {
                return false;
            }

            public long CurrentKey { get { throw new InvalidOperationException("Invalid position, cannot read past end of tree"); } }
            public Slice Value { get { throw new InvalidOperationException("Invalid position, cannot read past end of tree"); } }
            ByteStringContext.Scope IFixedSizeIterator.Value(out Slice slice)
            {
                slice = new Slice();
                return new ByteStringContext<ByteStringMemoryCache>.Scope();
            }

            public bool MoveNext()
            {
                return false;
            }

            public bool MovePrev()
            {
                return false;
            }

            public void Dispose()
            {
            }

            public ValueReader CreateReaderForCurrent()
            {
                throw new InvalidOperationException("No current page");
            }

            public bool Skip(int count)
            {
                return false;
            }
        }

        public class EmbeddedIterator : IFixedSizeIterator
        {
            private readonly FixedSizeTree _fst;
            private readonly ByteStringContext _allocator;
            private int _pos;
            private FixedSizeTreeHeader.Embedded* _header;
            private byte* _dataStart;
            private int _changesAtStart;

            public EmbeddedIterator(FixedSizeTree fst)
            {
                _fst = fst;
                _allocator = fst._tx.Allocator;
                _changesAtStart = _fst._changes;
                var ptr = _fst._parent.DirectRead(_fst._treeName);
                _header = (FixedSizeTreeHeader.Embedded*)ptr;
                _dataStart = ptr + sizeof(FixedSizeTreeHeader.Embedded);
            }

            public bool SeekToLast()
            {
                if (_header == null)
                    return false;
                _pos = _header->NumberOfEntries - 1;
                return true;
            }

            public bool Seek(long key)
            {
                if (_header == null)
                    return false;
                _pos = _fst.BinarySearch(_dataStart, _header->NumberOfEntries, key, _fst._entrySize);
                if (_fst._lastMatch > 0)
                    _pos++; // We didn't find the key.
                return _pos != _header->NumberOfEntries;
            }

            public long CurrentKey
            {
                get
                {
                    if (_pos >= _header->NumberOfEntries)
                        throw new InvalidOperationException("Invalid position, cannot read past end of tree");
                    return FixedSizeTreePage.GetEntry(_dataStart, _pos, _fst._entrySize)->Key;
                }
            }

            public long NumberOfEntriesLeft
            {
                get
                {
                    if (_fst._lastMatch == 0)
                        return _header->NumberOfEntries - _pos - 1;

                    return _header->NumberOfEntries - _pos;
                }
            }

            public ByteStringContext.Scope Value(out Slice slice)
            {
                if (_pos == _header->NumberOfEntries)
                    throw new InvalidOperationException("Invalid position, cannot read past end of tree");

                return Slice.External(_allocator, _dataStart + (_pos*_fst._entrySize) + sizeof(long), _fst._valSize, out slice);
            }

            public bool MovePrev()
            {
                AssertNoChanges();
                return --_pos >= 0;
            }

            private void AssertNoChanges()
            {
                if (_changesAtStart != _fst._changes)
                    throw new InvalidOperationException("You cannot perform modifications to tree when iterator is opened.");
            }

            public bool MoveNext()
            {
                AssertNoChanges();
                return ++_pos < _header->NumberOfEntries;
            }

            public ValueReader CreateReaderForCurrent()
            {
                return new ValueReader(_dataStart + (_pos * _fst._entrySize) + sizeof(long), _fst._valSize);
            }

            public bool Skip(int count)
            {
                if (count != 0)
                    _pos += count;

                return _pos < _header->NumberOfEntries;
            }

            public void Dispose()
            {
            }
        }

        public class LargeIterator : IFixedSizeIterator
        {
            private readonly FixedSizeTree _parent;
            private readonly ByteStringContext _allocator;
            private FixedSizeTreePage _currentPage;
            private int _changesAtStart;

            public LargeIterator(FixedSizeTree parent)
            {
                _parent = parent;
                _allocator = parent._tx.Allocator;               
                _changesAtStart = _parent._changes;
            }

            private void AssertNoChanges()
            {
                if (_changesAtStart != _parent._changes)
                    throw new InvalidOperationException("You cannot perform modifications to tree when iterator is opened.");
            }

            public void Dispose()
            {

            }

            public bool Seek(long key)
            {
                _currentPage = _parent.FindPageFor(key);
                return _currentPage.LastMatch <= 0 || MoveNext();
            }

            public bool SeekToLast()
            {
                _currentPage = _parent.FindPageFor(long.MaxValue);
                return true;
            }

            public long CurrentKey
            {
                get
                {
                    if (_currentPage == null)
                        throw new InvalidOperationException("No current page was set");

                    return _currentPage.GetKey(_currentPage.LastSearchPosition);
                }
            }

            public ByteStringContext.Scope Value(out Slice slice)
            {
                if (_currentPage == null)
                    throw new InvalidOperationException("No current page was set");

                return Slice.External(_allocator,
                    _currentPage.Pointer + _currentPage.StartPosition +
                    (_parent._entrySize*_currentPage.LastSearchPosition) + sizeof(long), _parent._valSize,
                    out slice);
            }

            public bool MoveNext()
            {
                AssertNoChanges();

                while (_currentPage != null)
                {
                    _currentPage.LastSearchPosition++;
                    if (_currentPage.LastSearchPosition < _currentPage.NumberOfEntries)
                    {
                        // run out of entries, need to select the next page...
                        while (_currentPage.IsBranch)
                        {
                            _parent._cursor.Push(_currentPage);
                            var childParentNumber = _currentPage.GetEntry(_currentPage.LastSearchPosition)->PageNumber;
                            _currentPage = _parent.GetReadOnlyPage(childParentNumber);

                            _currentPage.LastSearchPosition = 0;
                        }
                        return true;// there is another entry in this page
                    }
                    if (_parent._cursor.Count == 0)
                        break;
                    _currentPage = _parent._cursor.Pop();
                }
                _currentPage = null;

                return false;
            }

            private bool MovePrev(int skip)
            {
                AssertNoChanges();

                if (_currentPage == null || _currentPage.IsLeaf == false)
                    throw new InvalidOperationException("No current page was set or is wasn't a leaf!");

                while (skip >= 0)
                {
                    var skipInPage = Math.Min(_currentPage.LastSearchPosition, skip);
                    skip -= skipInPage;
                    _currentPage.LastSearchPosition -= skipInPage;
                    if (skip == 0)
                    {
                        return true;
                    }
                    
                    while (true)
                    {
                        if (_parent._cursor.TryPeek(out var parent) == false)
                            return false;
                        
                        parent.LastSearchPosition--;
                        
                        if (parent.LastSearchPosition < 0)
                        {
                            _parent._cursor.Pop();
                            continue;
                        }
                        
                        var nextChildPageNumber = parent.GetEntry(parent.LastSearchPosition)->PageNumber;
                        var childPage = _parent.GetReadOnlyPage(nextChildPageNumber);
                        
                        // we move one beyond the end of elements because in both cases
                        // (branch and leaf) we first decrement and then run it
                        childPage.LastSearchPosition = childPage.NumberOfEntries;
                        if (childPage.IsBranch)
                        {
                            _parent._cursor.Push(childPage);
                            continue;
                        }

                        _currentPage = childPage;
                        break;
                    }
                }
                _currentPage = null;

                return false;
            }
            
            private bool MoveNext(int skip)
            {
                AssertNoChanges();

                if (_currentPage == null || _currentPage.IsLeaf == false)
                    throw new InvalidOperationException("No current page was set or is wasn't a leaf!");

                while (skip >= 0)
                {
                    var skipInPage = Math.Min(_currentPage.NumberOfEntries - _currentPage.LastSearchPosition, skip);
                    skip -= skipInPage;
                    _currentPage.LastSearchPosition += skipInPage;
                    if (skip == 0)
                    {
                        if (_currentPage.LastSearchPosition >= _currentPage.NumberOfEntries)
                            return MoveNext();
                        return true;
                    }
                    
                    while (true)
                    {
                        if (_parent._cursor.TryPeek(out var parent) == false)
                            return false;
                        
                        parent.LastSearchPosition++;
                        
                        if (parent.LastSearchPosition >= parent.NumberOfEntries)
                        {
                            _parent._cursor.Pop();
                            continue;
                        }
                        
                        var nextChildPageNumber = parent.GetEntry(parent.LastSearchPosition)->PageNumber;
                        var childPage = _parent.GetReadOnlyPage(nextChildPageNumber);

                        if (childPage.IsBranch)
                        {
                            // we set it to negative one so the first
                            // call will increment that to zero
                            childPage.LastSearchPosition = -1;
                            _parent._cursor.Push(childPage);
                            continue;
                        }
                        else
                        {
                            childPage.LastSearchPosition = 0;
                        }
                        
                        _currentPage = childPage;
                        break;
                    }
                }
                _currentPage = null;

                return false;
            }


            public bool MovePrev()
            {
                AssertNoChanges();

                if (_currentPage == null)
                    throw new InvalidOperationException("No current page was set");

                while (_currentPage != null)
                {
                    _currentPage.LastSearchPosition--;
                    if (_currentPage.LastSearchPosition >= 0)
                    {
                        // run out of entries, need to select the next page...
                        while (_currentPage.IsBranch)
                        {
                            _parent._cursor.Push(_currentPage);
                            var childParentNumber = _currentPage.GetEntry(_currentPage.LastSearchPosition)->PageNumber;
                            _currentPage = _parent.GetReadOnlyPage(childParentNumber);

                            _currentPage.LastSearchPosition = _currentPage.NumberOfEntries - 1;
                        }
                        return true;// there is another entry in this page
                    }
                    if (_parent._cursor.Count == 0)
                        break;
                    _currentPage = _parent._cursor.Pop();
                }
                _currentPage = null;

                return false;
            }


            public ValueReader CreateReaderForCurrent()
            {
                if (_currentPage == null)
                    throw new InvalidOperationException("No current page was set");

                return new ValueReader(_currentPage.Pointer + _currentPage.StartPosition + (_parent._entrySize * _currentPage.LastSearchPosition) + sizeof(long), _parent._valSize);
            }

            public bool Skip(int count)
            {
                if (count > 0)
                {
                    if (MoveNext(count) == false)
                        return false;
                }
                else
                {
                    if (MovePrev(Math.Abs(count)) == false)
                        return false;
                }

                var seek = _currentPage != null && _currentPage.LastSearchPosition != _currentPage.NumberOfEntries;
                if (seek == false)
                    _currentPage = null;
                return seek;
            }
        }
    }
}
