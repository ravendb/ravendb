using System;
using Voron.Impl;

namespace Voron.Data.BTrees
{
    public struct TreeCursorRef
    {
        public readonly TreeCursor Cursor;

        public TreeCursorRef(TreeCursor cursor)
        {
            this.Cursor = cursor;
        }
    }

    public unsafe struct TreeCursorConstructor
    {
        private readonly Tree _tree;
        private readonly LowLevelTransaction _llt;
        private readonly long[] _cursorPath;
        private readonly long _lastFoundPageNumber;
        private readonly TreePage _pageCopy;
        private readonly TreeCursor _current;

        public TreeCursorConstructor(TreeCursor cursor)
        {
            this._current = cursor;

            this._llt = null;
            this._tree = null;
            this._pageCopy = null;
            this._cursorPath = null;
            this._lastFoundPageNumber = 0;

        }

        public TreeCursorConstructor(LowLevelTransaction llt, Tree tree, TreePage pageCopy, long[] cursorPath, long lastFoundPageNumber)
        {
            this._llt = llt;
            this._tree = tree;
            this._pageCopy = pageCopy;
            this._cursorPath = cursorPath;
            this._lastFoundPageNumber = lastFoundPageNumber;

            this._current = null;
        }

        public TreeCursor Build(Slice key)
        {
            if (_current != null)
                return _current;

            var c = new TreeCursor();
            foreach (var p in _cursorPath)
            {
                if (p == _lastFoundPageNumber)
                {
                    c.Push(_pageCopy);
                }
                else
                {
                    var cursorPage = _tree.GetReadOnlyTreePage(p);
                    if (key.Options == SliceOptions.Key)
                    {
                        if (cursorPage.Search(_llt, key) != null && cursorPage.LastMatch != 0)
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
            return c;
        }
    }
}
