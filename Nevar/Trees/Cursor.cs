using System;
using System.Collections.Generic;
using System.Linq;
using Nevar.Debugging;
using Nevar.Impl;

namespace Nevar.Trees
{
    public class Cursor : IDisposable
    {
        public LinkedList<Page> Pages = new LinkedList<Page>();
        private Dictionary<long, Page> pagesByNum = new Dictionary<long, Page>(); 


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

        public void Push(Page p)
        {
            Pages.AddFirst(p);
            pagesByNum.Add(p.PageNumber, p);
        }

        public Page Pop()
        {
            if (Pages.Count == 0)
            {
                throw new InvalidOperationException("No page to pop");
            }
            Page p = Pages.First.Value;
            Pages.RemoveFirst();
            pagesByNum.Remove(p.PageNumber);
            return p;
        }

	    public Page GetPage(long p)
	    {
	        Page page;
	        if (pagesByNum.TryGetValue(p, out page))
	            return page;
	        return null;
	    }

        public void Dispose()
        {
        }
    }
}
