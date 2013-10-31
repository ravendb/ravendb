using System;
using System.Collections.Generic;

namespace Voron.Trees
{
    public class Cursor : IDisposable
    {
        public LinkedList<Page> Pages = new LinkedList<Page>();
        private readonly Dictionary<long, Page> _pagesByNum = new Dictionary<long, Page>(); 

        public void Update(LinkedListNode<Page> node, Page newVal)
        {
            if (node.Value.PageNumber == newVal.PageNumber)
            {
                _pagesByNum[node.Value.PageNumber] = newVal;
                node.Value = newVal;
                return;
            }
            _pagesByNum[node.Value.PageNumber] = newVal;
            _pagesByNum.Add(newVal.PageNumber, newVal);
            node.Value = newVal;
        }

        public Page ParentPage
        {
            get
            {
                LinkedListNode<Page> linkedListNode = Pages.First;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No pages in cursor");
                linkedListNode = linkedListNode.Next;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No parent page in cursor");
                return linkedListNode.Value;
            }
        }

        public Page CurrentPage
        {
            get
            {
                LinkedListNode<Page> linkedListNode = Pages.First;
                if (linkedListNode == null)
                    throw new InvalidOperationException("No pages in cursor");
                return linkedListNode.Value;
            }
        }

        public int PageCount
        {
            get { return Pages.Count; }
        }

        public void Push(Page p)
        {
            Pages.AddFirst(p);
            _pagesByNum.Add(p.PageNumber, p);
        }

        public Page Pop()
        {
            if (Pages.Count == 0)
            {
                throw new InvalidOperationException("No page to pop");
            }
            Page p = Pages.First.Value;
            Pages.RemoveFirst();
            _pagesByNum.Remove(p.PageNumber);
            return p;
        }

	    public Page GetPage(long p)
	    {
	        Page page;
	        if (_pagesByNum.TryGetValue(p, out page))
	            return page;
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
