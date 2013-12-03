using System;
using System.Collections.Generic;

namespace Voron.Trees
{
    public class Cursor : IDisposable
    {
	    public class LazyCursorPage
	    {
		    public long Number;
		    public Lazy<Page> Page;
	    }

		public LinkedList<LazyCursorPage> Pages = new LinkedList<LazyCursorPage>();
		private readonly Dictionary<long, Lazy<Page>> _pagesByNum = new Dictionary<long, Lazy<Page>>();

		public void Update(LinkedListNode<LazyCursorPage> node, Page newVal)
        {
            if (node.Value.Number == newVal.PageNumber)
            {
                _pagesByNum[node.Value.Number] = new Lazy<Page>(() => newVal);
                node.Value.Page = new Lazy<Page>(() => newVal);
                return;
            }
            _pagesByNum[node.Value.Number] = new Lazy<Page>(() => newVal);
            _pagesByNum.Add(newVal.PageNumber, new Lazy<Page>(() => newVal));
            node.Value.Page = new Lazy<Page>(() => newVal);
        }

        public long ParentPage
        {
            get
            {
				LinkedListNode<LazyCursorPage> linkedListNode = Pages.First;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No pages in cursor");
                linkedListNode = linkedListNode.Next;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No parent page in cursor");
                return linkedListNode.Value.Number;
            }
        }

        public long CurrentPage
        {
            get
            {
				LinkedListNode<LazyCursorPage> linkedListNode = Pages.First;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No pages in cursor");
                return linkedListNode.Value.Number;
            }
        }

        public int PageCount
        {
            get { return Pages.Count; }
        }

        public void Push(Page p)
        {
            Pages.AddFirst(new LazyCursorPage
	            {
		            Number = p.PageNumber,
					Page = new Lazy<Page>(() => p)
	            });

            _pagesByNum.Add(p.PageNumber, new Lazy<Page>(() => p));
        }

		public void Push(LazyCursorPage p)
		{
			Pages.AddFirst(p);
			_pagesByNum.Add(p.Number, p.Page);
		}

        public void Pop()
        {
            if (Pages.Count == 0)
            {
                throw new InvalidOperationException("No page to pop");
            }
            LazyCursorPage p = Pages.First.Value;
            Pages.RemoveFirst();
            _pagesByNum.Remove(p.Number);
        }

		public Page PopAndGet()
		{
			if (Pages.Count == 0)
			{
				throw new InvalidOperationException("No page to pop");
			}
			LazyCursorPage p = Pages.First.Value;
			Pages.RemoveFirst();
			_pagesByNum.Remove(p.Number);
			return p.Page.Value;
		}

	    public Page GetPage(long p)
	    {
	        Lazy<Page> lazyPage;
	        if (_pagesByNum.TryGetValue(p, out lazyPage))
	        {
		        return lazyPage.Value;
	        }
	        return null;
	    }

        public void Dispose()
        {
        }

        public void Clear()
        {
            _pagesByNum.Clear();
            Pages.Clear();
        }
    }
}
