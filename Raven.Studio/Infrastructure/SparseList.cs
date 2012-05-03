using System;
using System.Collections.ObjectModel;

namespace Raven.Studio.Infrastructure
{
    /// <summary>
    /// A list which can support a huge virtual item count
    /// by assuming that many items are never accessed. Space for items is allocated
    /// in pages.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    public class SparseList<T> where T:class 
    {
        private readonly int _pageSize;
        private readonly PageList _allocatedPages;
        private Page _currentPage;
 
        public SparseList(int pageSize)
        {
            _pageSize = pageSize;
            _allocatedPages = new PageList(_pageSize);
        }

        /// <remarks>This method is optimised for sequential access. I.e. it performs
        /// best when getting and setting indicies in the same locality</remarks>
        public T this[int index]
        {
            get
            {
                var pageAndSubIndex = EnsureCurrentPage(index);

                return _currentPage[pageAndSubIndex.SubIndex];
            }
            set
            {
                var pageAndSubIndex = EnsureCurrentPage(index);

                _currentPage[pageAndSubIndex.SubIndex] = value;
            }
        }

        private PageAndSubIndex EnsureCurrentPage(int index)
        {
            var pageAndSubIndex = new PageAndSubIndex(index / _pageSize, index % _pageSize);

            if (_currentPage == null || _currentPage.PageIndex != pageAndSubIndex.PageIndex)
            {
                _currentPage = _allocatedPages.GetOrCreatePage(pageAndSubIndex.PageIndex);
            }

            return pageAndSubIndex;
        }

        public void RemoveRange(int firstIndex, int count)
        {
            var firstItem = new PageAndSubIndex(firstIndex / _pageSize, firstIndex % _pageSize);
            if (firstItem.SubIndex + count > _pageSize)
            {
                throw new NotImplementedException("RemoveRange is only implemented to work within page boundaries");
            }

            if (_allocatedPages.Contains(firstItem.PageIndex))
            {
                if (_allocatedPages[firstItem.PageIndex].Trim(firstItem.SubIndex, count))
                {
                    _allocatedPages.Remove(firstItem.PageIndex);
                }
            }
        }

        private struct PageAndSubIndex
        {
            private readonly int _pageIndex;
            private readonly int _subIndex;

            public PageAndSubIndex(int pageIndex, int subIndex)
            {
                _pageIndex = pageIndex;
                _subIndex = subIndex;
            }

            public int PageIndex
            {
                get { return _pageIndex; }
            }

            public int SubIndex
            {
                get { return _subIndex; }
            }
        }

        private class Page
        {
            private readonly int _pageIndex;
            private readonly T[] _items;

            public Page(int pageIndex, int pageSize)
            {
                _pageIndex = pageIndex;
                _items = new T[pageSize];
            }

            public int PageIndex
            {
                get { return _pageIndex; }
            }

            public T this[int index]
            {
                get
                {
                    return _items[index];
                }
                set
                {
                    _items[index] = value;
                }
            }

            public bool Trim(int firstIndex, int count)
            {
                for (int i = firstIndex; i < firstIndex + count; i++)
                {
                    _items[i] = default(T);
                }

                for (int i = 0; i < _items.Length; i++)
                {
                    if (_items[i] != null)
                    {
                        return false;
                    }
                }

                return true;
            }
        }

        private class PageList : KeyedCollection<int, Page>
        {
            private readonly int _pageSize;

            public PageList(int pageSize)
            {
                _pageSize = pageSize;
            }

            protected override int GetKeyForItem(Page item)
            {
                return item.PageIndex;
            }

            public Page GetOrCreatePage(int pageIndex)
            {
                if (!Contains(pageIndex))
                {
                    Add(new Page(pageIndex, _pageSize));
                }

                return this[pageIndex];
            }
        }
    }
}
