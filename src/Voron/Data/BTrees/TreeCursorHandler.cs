#nullable enable

using System;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public readonly unsafe struct TreeCursorConstructor : IDisposable
    {
        public readonly struct DisposeContext(TreeCursor? context) : IDisposable
        {
            private readonly TreeCursor? _context = context;

            public void Dispose()
            {
                _context?.Dispose();
            }
        }
        
        private readonly Tree? _tree;
        private readonly LowLevelTransaction? _llt;
        private readonly long[]? _cursorPath;
        private readonly long _lastFoundPageNumber;
        private readonly TreePage? _pageCopy;
        private readonly TreeCursor? _current;

        public TreeCursor? Cursor => _current;

        public TreeCursorConstructor(TreeCursor cursor)
        {
            _current = cursor;

            _llt = null;
            _tree = null;
            _pageCopy = null;
            _cursorPath = null;
            _lastFoundPageNumber = 0;
        }

        public TreeCursorConstructor(LowLevelTransaction llt, Tree tree, TreePage pageCopy, long[] cursorPath, long lastFoundPageNumber)
        {
            _llt = llt;
            _tree = tree;
            _pageCopy = pageCopy;
            _cursorPath = cursorPath;
            _lastFoundPageNumber = lastFoundPageNumber;

            _current = null;
        }

        public DisposeContext Build(Slice key, out TreeCursor cursor)
        {
            if (_current != null)
            {
                cursor = _current;
                return new DisposeContext(null);
            }

            var c = new TreeCursor();
            foreach (var p in _cursorPath!)
            {
                if (p == _lastFoundPageNumber)
                {
                    c.Push(_pageCopy);
                }
                else
                {
                    var cursorPage = _tree!.GetReadOnlyTreePage(p);
                    if (key.Options == SliceOptions.Key)
                    {
                        cursorPage.Search(_llt, key);
                        if (cursorPage.LastMatch != 0)
                            cursorPage.LastSearchPosition--;
                    }
                    else if (key.Options == SliceOptions.BeforeAllKeys)
                    {
                        cursorPage.LastSearchPosition = 0;
                    }
                    else if (key.Options == SliceOptions.AfterAllKeys)
                    {
                        cursorPage.LastSearchPosition = (ushort)(cursorPage.NumberOfEntries - 1);
                    }
                    else throw new ArgumentException();

                    c.Push(cursorPage);
                }
            }

            cursor = c;
            return new DisposeContext(cursor);
        }

        public void Dispose()
        {
            _current?.Dispose();
        }
    }
}
